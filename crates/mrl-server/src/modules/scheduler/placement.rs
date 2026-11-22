//! `SingleNodePacking`: every role of a job on one machine.
//!
//! Filters machines by health, pinned machines, platform, CPU and memory; filters GPUs by
//! maker, memory, architecture and model. Each role gets its GPUs (largest role first),
//! preferring a set where every GPU has an NVLink to every other. Among machines that fit,
//! the best fit wins: fewest free GPUs left, then least memory left (packing keeps big
//! machines whole for big jobs). Machines to avoid (an earlier attempt failed there) are used
//! only when nothing else fits.

use super::pipeline::{Demand, Fit, Placement, PlacementHints, PlacementStrategy, RoleDemand};
use crate::modules::compute::{GpuSlot, NodeCapacity};
use uuid::Uuid;

/// See the module docs.
pub struct SingleNodePacking;

/// Above this many candidate GPU sets the NVLink search is skipped (lowest indices win).
const MAX_SETS: u64 = 5_000;

fn gib(b: u64) -> String {
    let g = b as f64 / f64::from(1u32 << 30);
    format!("{g:.1} GiB")
}

/// How many ways to pick `k` of `n`, saturating.
fn choose(n: usize, k: usize) -> u64 {
    let k = k.min(n.saturating_sub(k));
    (0..k).fold(1u64, |acc, i| {
        acc.saturating_mul((n - i) as u64) / (i as u64 + 1)
    })
}

/// True when every GPU in `set` has an NVLink to every other.
fn nvlinked(set: &[&GpuSlot]) -> bool {
    set.iter().all(|a| {
        set.iter()
            .all(|b| a.index == b.index || a.nvlink_peers.contains(&b.index))
    })
}

/// `k` GPUs of `pool`: an NVLink-connected set when one exists, else the lowest indices.
fn pick<'a>(pool: &[&'a GpuSlot], k: usize) -> Option<Vec<&'a GpuSlot>> {
    if pool.len() < k {
        return None;
    }
    if k > 1 && choose(pool.len(), k) <= MAX_SETS {
        // ponytail: brute-force combinations; machines have ≤ 16 GPUs, capped at MAX_SETS.
        let mut idx: Vec<usize> = (0..k).collect();
        loop {
            let set: Vec<&GpuSlot> = idx.iter().map(|&i| pool[i]).collect();
            if nvlinked(&set) {
                return Some(set);
            }
            // Next combination in lexicographic order.
            let mut i = k;
            loop {
                if i == 0 {
                    return Some(pool[..k].to_vec());
                }
                i -= 1;
                if idx[i] < pool.len() - k + i {
                    break;
                }
            }
            idx[i] += 1;
            for j in i + 1..k {
                idx[j] = idx[j - 1] + 1;
            }
        }
    }
    Some(pool[..k].to_vec())
}

/// Assigns GPUs to every role on `node`, from free GPUs only when `now`. `Err`: why not.
fn assign(demand: &Demand, node: &NodeCapacity, now: bool) -> Result<Placement, String> {
    let (cpu, mem) = if now {
        (node.cpu_millis_free, node.mem_bytes_free)
    } else {
        (node.cpu_millis_total, node.mem_bytes_total)
    };
    if demand.cpu_millis() > cpu {
        return Err(format!(
            "{:.1} CPU cores (the machine has {:.1} {})",
            demand.cpu_millis() as f64 / 1000.0,
            cpu as f64 / 1000.0,
            if now { "free" } else { "in all" }
        ));
    }
    if demand.mem_bytes() > mem {
        return Err(format!(
            "{} of memory (the machine has {} {})",
            gib(demand.mem_bytes()),
            gib(mem),
            if now { "free" } else { "in all" }
        ));
    }
    let mut order: Vec<usize> = (0..demand.roles.len()).collect();
    order.sort_by_key(|&i| std::cmp::Reverse(demand.roles[i].gpu_count));
    let mut taken: Vec<Uuid> = Vec::new();
    let mut chosen: Vec<Vec<GpuSlot>> = vec![Vec::new(); demand.roles.len()];
    for i in order {
        let role = &demand.roles[i];
        let k = role.gpu_count as usize;
        if k == 0 {
            continue;
        }
        let pool: Vec<&GpuSlot> = node
            .gpus
            .iter()
            .filter(|g| (!now || g.free) && !taken.contains(&g.id) && role.accepts(g))
            .collect();
        let set = pick(&pool, k).ok_or_else(|| {
            format!(
                "{k} GPU(s){} for the `{}` role ({} {})",
                constraints(role),
                role.role,
                pool.len(),
                if now { "free" } else { "match" }
            )
        })?;
        taken.extend(set.iter().map(|g| g.id));
        chosen[i] = set.into_iter().cloned().collect();
    }
    Ok(Placement {
        node_id: node.node_id,
        roles: demand
            .roles
            .iter()
            .zip(chosen)
            .map(|(r, g)| (r.role.clone(), g))
            .collect(),
        cpu_millis: demand.cpu_millis(),
        mem_bytes: demand.mem_bytes(),
    })
}

fn constraints(r: &RoleDemand) -> String {
    let mut parts = Vec::new();
    if let Some(v) = &r.vendor {
        parts.push(format!("made by {v}"));
    }
    if let Some(m) = r.min_vram_bytes {
        parts.push(format!("with at least {} each", gib(m)));
    }
    if !r.arch.is_empty() {
        parts.push(format!("of type {}", r.arch.join(" or ")));
    }
    if !r.models.is_empty() {
        parts.push(format!("model {}", r.models.join(" or ")));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(" {}", parts.join(", "))
    }
}

/// True when `node` passes the role filters that are not about free resources.
fn allowed(demand: &Demand, node: &NodeCapacity) -> Result<(), String> {
    for r in &demand.roles {
        if !r.node_ids.is_empty() && !r.node_ids.contains(&node.node_id) {
            return Err("one of the machines it is pinned to".into());
        }
        if let Some(p) = r.platform {
            if node.platform != Some(p) {
                return Err(format!("a machine running {p:?}"));
            }
        }
    }
    Ok(())
}

impl PlacementStrategy for SingleNodePacking {
    fn name(&self) -> &'static str {
        super::pipeline::DEFAULT_PLACEMENT
    }

    fn place(&self, demand: &Demand, nodes: &[NodeCapacity], hints: &PlacementHints) -> Fit {
        if let Some(r) = demand.roles.iter().find(|r| r.replicas > 1) {
            return Fit::Never(format!(
                "The `{}` role asks for {} machines, but MasterRL runs a job on one machine for now. Ask for 1 machine with more GPUs.",
                r.role, r.replicas
            ));
        }
        if nodes.is_empty() {
            return Fit::Never(
                "This pool has no machines yet. Add a machine to the pool to run jobs.".into(),
            );
        }
        let mut best: Option<((bool, u64, u64, Uuid), Placement)> = None;
        let mut could = false;
        let mut why_never = Vec::new();
        let mut why_busy = Vec::new();
        for node in nodes {
            if let Err(w) =
                allowed(demand, node).and_then(|()| assign(demand, node, false).map(|_| ()))
            {
                why_never.push(w);
                continue;
            }
            could = true;
            if !node.schedulable {
                why_busy.push("offline, draining or held back".to_owned());
                continue;
            }
            match assign(demand, node, true) {
                Ok(p) => {
                    let used: Vec<Uuid> = p.gpu_ids();
                    let left_gpus = node
                        .gpus
                        .iter()
                        .filter(|g| g.free && !used.contains(&g.id))
                        .count() as u64;
                    let key = (
                        hints.avoid_nodes.contains(&node.node_id),
                        left_gpus,
                        node.mem_bytes_free.saturating_sub(p.mem_bytes),
                        node.node_id,
                    );
                    if best.as_ref().is_none_or(|(k, _)| key < *k) {
                        best = Some((key, p));
                    }
                }
                Err(w) => why_busy.push(format!("needs {w}")),
            }
        }
        match best {
            Some((_, p)) => Fit::Placed(p),
            None if could => Fit::Busy(format!(
                "Waiting for a machine: every machine that can run this job is busy right now ({}). It starts when one is free.",
                why_busy.first().map_or("busy", String::as_str)
            )),
            None => Fit::Never(format!(
                "No machine in this pool can run this job: it needs {}. Change the resources the job asks for, or add a machine that has them.",
                why_never
                    .first()
                    .map_or("more than any machine has", String::as_str)
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::scheduler::pipeline::RoleDemand;

    fn gpu(node: u128, index: u32, vram_gib: u64, free: bool, peers: &[u32]) -> GpuSlot {
        GpuSlot {
            id: Uuid::from_u128(node * 100 + u128::from(index)),
            index,
            vendor: "nvidia".into(),
            model: "NVIDIA A100".into(),
            arch: "sm_80".into(),
            vram_bytes: vram_gib << 30,
            nvlink_peers: peers.to_vec(),
            free,
        }
    }

    fn node(id: u128, gpus: Vec<GpuSlot>) -> NodeCapacity {
        NodeCapacity {
            node_id: Uuid::from_u128(id),
            pool_id: Uuid::nil(),
            schedulable: true,
            platform: Some(mrl_domain::resources::Platform::Linux),
            gpus,
            cpu_millis_total: 32_000,
            cpu_millis_free: 32_000,
            mem_bytes_total: 256 << 30,
            mem_bytes_free: 256 << 30,
        }
    }

    fn want(gpus: u32) -> Demand {
        Demand {
            roles: vec![RoleDemand {
                role: "trainer".into(),
                replicas: 1,
                gpu_count: gpus,
                cpu_millis: 1000,
                mem_bytes: 1 << 30,
                ..RoleDemand::default()
            }],
        }
    }

    fn placed(f: Fit) -> Placement {
        match f {
            Fit::Placed(p) => p,
            other => panic!("expected a placement, got {other:?}"),
        }
    }

    /// A machine with `free` of `total` GPUs free.
    fn fragmented(id: u128, total: u32, free: u32) -> NodeCapacity {
        node(
            id,
            (0..total)
                .map(|i| gpu(id, i, 80, i >= total - free, &[]))
                .collect(),
        )
    }

    #[test]
    fn best_fit_packs_fragmented_machines() {
        let nodes = vec![
            fragmented(1, 8, 4),
            fragmented(2, 2, 2),
            fragmented(3, 8, 8),
        ];
        let s = SingleNodePacking;
        let h = PlacementHints::default();
        // 2 GPUs: the machine left with no free GPUs wins.
        assert_eq!(
            placed(s.place(&want(2), &nodes, &h)).node_id,
            Uuid::from_u128(2)
        );
        // 3 GPUs: the 4-free machine (1 left) beats the empty one (5 left).
        let p = placed(s.place(&want(3), &nodes, &h));
        assert_eq!(p.node_id, Uuid::from_u128(1));
        assert!(p.roles[0].1.iter().all(|g| g.free));
        // 8 GPUs: only the empty machine.
        assert_eq!(
            placed(s.place(&want(8), &nodes, &h)).node_id,
            Uuid::from_u128(3)
        );
        // 9: never (no machine has 9).
        assert!(matches!(s.place(&want(9), &nodes, &h), Fit::Never(_)));
    }

    #[test]
    fn busy_versus_never() {
        let s = SingleNodePacking;
        let h = PlacementHints::default();
        let nodes = vec![fragmented(1, 4, 1)];
        match s.place(&want(2), &nodes, &h) {
            Fit::Busy(why) => assert!(why.contains("busy"), "{why}"),
            other => panic!("{other:?}"),
        }
        let mut offline = fragmented(2, 4, 4);
        offline.schedulable = false;
        assert!(matches!(s.place(&want(2), &[offline], &h), Fit::Busy(_)));
        match s.place(&want(1), &[], &h) {
            Fit::Never(why) => assert!(why.contains("no machines"), "{why}"),
            other => panic!("{other:?}"),
        }
        let mut multi = want(1);
        multi.roles[0].replicas = 2;
        assert!(matches!(s.place(&multi, &nodes, &h), Fit::Never(_)));
    }

    #[test]
    fn constraints_filter_gpus_and_machines() {
        let s = SingleNodePacking;
        let h = PlacementHints::default();
        let mut small = fragmented(1, 4, 4);
        for g in &mut small.gpus {
            g.vram_bytes = 24 << 30;
            g.arch = "sm_86".into();
            g.model = "RTX 4090".into();
        }
        let big = fragmented(2, 4, 4);
        let nodes = vec![small, big];
        let mut d = want(2);
        d.roles[0].min_vram_bytes = Some(40 << 30);
        assert_eq!(placed(s.place(&d, &nodes, &h)).node_id, Uuid::from_u128(2));
        let mut d = want(1);
        d.roles[0].arch = vec!["sm_86".into()];
        assert_eq!(placed(s.place(&d, &nodes, &h)).node_id, Uuid::from_u128(1));
        let mut d = want(1);
        d.roles[0].models = vec!["4090".into()];
        assert_eq!(placed(s.place(&d, &nodes, &h)).node_id, Uuid::from_u128(1));
        let mut d = want(1);
        d.roles[0].vendor = Some("amd".into());
        match s.place(&d, &nodes, &h) {
            Fit::Never(why) => assert!(why.contains("made by amd"), "{why}"),
            other => panic!("{other:?}"),
        }
        // Pinned to machine 1 only.
        let mut d = want(1);
        d.roles[0].node_ids = vec![Uuid::from_u128(1)];
        assert_eq!(placed(s.place(&d, &nodes, &h)).node_id, Uuid::from_u128(1));
        // Platform.
        let mut d = want(1);
        d.roles[0].platform = Some(mrl_domain::resources::Platform::AppleSilicon);
        assert!(matches!(s.place(&d, &nodes, &h), Fit::Never(_)));
        // CPU and memory counters.
        let mut d = want(1);
        d.roles[0].mem_bytes = 300 << 30;
        match s.place(&d, &nodes, &h) {
            Fit::Never(why) => assert!(why.contains("memory"), "{why}"),
            other => panic!("{other:?}"),
        }
        let mut tight = nodes.clone();
        for n in &mut tight {
            n.cpu_millis_free = 500;
        }
        assert!(matches!(s.place(&want(1), &tight, &h), Fit::Busy(_)));
    }

    #[test]
    fn nvlink_connected_sets_are_preferred() {
        // GPUs 0,1 linked to nothing; 2,3 linked to each other.
        let n = node(
            1,
            vec![
                gpu(1, 0, 80, true, &[]),
                gpu(1, 1, 80, true, &[]),
                gpu(1, 2, 80, true, &[3]),
                gpu(1, 3, 80, true, &[2]),
            ],
        );
        let p = placed(SingleNodePacking.place(
            &want(2),
            std::slice::from_ref(&n),
            &PlacementHints::default(),
        ));
        assert_eq!(p.gpu_indices(), vec![2, 3]);
        // Without links the lowest indices win.
        let mut flat = n;
        for g in &mut flat.gpus {
            g.nvlink_peers.clear();
        }
        let p = placed(SingleNodePacking.place(&want(2), &[flat], &PlacementHints::default()));
        assert_eq!(p.gpu_indices(), vec![0, 1]);
    }

    #[test]
    fn several_roles_share_one_machine_without_sharing_gpus() {
        let d = Demand {
            roles: vec![
                RoleDemand {
                    role: "trainer".into(),
                    replicas: 1,
                    gpu_count: 2,
                    ..RoleDemand::default()
                },
                RoleDemand {
                    role: "rollout".into(),
                    replicas: 1,
                    gpu_count: 1,
                    ..RoleDemand::default()
                },
            ],
        };
        let p =
            placed(SingleNodePacking.place(&d, &[fragmented(1, 4, 3)], &PlacementHints::default()));
        let ids = p.gpu_ids();
        assert_eq!(ids.len(), 3);
        let mut dedup = ids.clone();
        dedup.sort();
        dedup.dedup();
        assert_eq!(dedup.len(), 3);
        assert_eq!(p.roles[0].0, "trainer");
        assert_eq!(p.roles[0].1.len(), 2);
        assert!(matches!(
            SingleNodePacking.place(&d, &[fragmented(1, 4, 2)], &PlacementHints::default()),
            Fit::Busy(_)
        ));
    }

    #[test]
    fn avoided_machines_are_a_last_resort() {
        let nodes = vec![fragmented(1, 2, 2), fragmented(2, 4, 4)];
        let h = PlacementHints {
            avoid_nodes: vec![Uuid::from_u128(1)],
        };
        assert_eq!(
            placed(SingleNodePacking.place(&want(2), &nodes, &h)).node_id,
            Uuid::from_u128(2)
        );
        let only = vec![fragmented(1, 2, 2)];
        assert_eq!(
            placed(SingleNodePacking.place(&want(2), &only, &h)).node_id,
            Uuid::from_u128(1)
        );
    }

    #[test]
    fn a_cpu_only_job_needs_no_gpu() {
        let p = placed(SingleNodePacking.place(
            &want(0),
            &[fragmented(1, 0, 0)],
            &PlacementHints::default(),
        ));
        assert!(p.gpu_ids().is_empty());
    }

    /// Simulator: random fragmented pools; every placement uses free, matching, distinct GPUs
    /// on one machine within its counters, and "never" is said only when no machine has
    /// enough GPUs in all.
    #[test]
    fn simulator_never_breaks_the_rules() {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move |m: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % m
        };
        for _ in 0..500 {
            let n = 1 + next(5) as u128;
            let mut nodes = Vec::new();
            for id in 1..=n {
                let total = next(9) as u32;
                let gpus = (0..total)
                    .map(|i| {
                        let peers: Vec<u32> =
                            (0..total).filter(|p| *p != i && next(2) == 0).collect();
                        gpu(id, i, 16 + next(70), next(3) != 0, &peers)
                    })
                    .collect();
                let mut c = node(id, gpus);
                c.cpu_millis_free = next(32_001);
                c.schedulable = next(5) != 0;
                nodes.push(c);
            }
            let mut d = want(next(9) as u32);
            d.roles[0].min_vram_bytes = (next(2) == 0).then(|| (16 + next(70)) << 30);
            match SingleNodePacking.place(&d, &nodes, &PlacementHints::default()) {
                Fit::Placed(p) => {
                    let node = nodes.iter().find(|x| x.node_id == p.node_id).unwrap();
                    assert!(node.schedulable);
                    assert!(p.cpu_millis <= node.cpu_millis_free);
                    let ids = p.gpu_ids();
                    assert_eq!(ids.len(), d.gpus() as usize);
                    for id in &ids {
                        let g = node.gpus.iter().find(|g| g.id == *id).unwrap();
                        assert!(g.free && d.roles[0].accepts(g));
                    }
                    let mut u = ids.clone();
                    u.sort();
                    u.dedup();
                    assert_eq!(u.len(), ids.len());
                }
                Fit::Never(_) => assert!(nodes.iter().all(|x| x
                    .gpus
                    .iter()
                    .filter(|g| d.roles[0].accepts(g))
                    .count()
                    < d.gpus() as usize)),
                Fit::Busy(_) => {}
            }
        }
    }
}
