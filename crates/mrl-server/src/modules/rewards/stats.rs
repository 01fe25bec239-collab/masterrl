//! Statistics the server computes from a REWARD_PREVIEW job's per-sample rows (tagged
//! `sampled`): distributions of the total and each component, failure rates, latency,
//! the reward-vs-length rank correlation, the learnability check (share of prompt groups
//! whose rewards have no spread, which give group-based methods like GRPO nothing to learn
//! from), and the two-version comparison used by regression and `POST /rewards:compare`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Most scatter points kept in a comparison.
pub const MAX_SCATTER: usize = 1000;
/// Bins of each histogram.
pub const BINS: usize = 20;

/// One scored sample, as the preview task writes it (a line of `rows.jsonl`).
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Row {
    /// Sample number (the same sample has the same number in both versions).
    pub i: u64,
    /// Prompt group (the completions of one prompt).
    #[serde(default)]
    pub group_id: String,
    /// Which version scored it: `a` (the version) or `b` (the other one).
    #[serde(default = "version_a")]
    pub version: String,
    /// The total, or `null` when masked.
    pub total: Option<f64>,
    /// `SCORED`, `MASKED` or `FALLBACK_APPLIED`.
    pub resolution: String,
    /// Worst component status, like `OK` or `TIMEOUT`.
    #[serde(default)]
    pub status: String,
    /// Raw component values.
    #[serde(default)]
    pub components: BTreeMap<String, Option<f64>>,
    /// Flags, like `gated`.
    #[serde(default)]
    pub flags: Vec<String>,
    /// Completion tokens.
    #[serde(default)]
    pub completion_tokens: u32,
    /// Scoring time of its batch per sample, in milliseconds.
    #[serde(default)]
    pub latency_ms: f64,
}

fn version_a() -> String {
    "a".into()
}

/// A distribution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[schema(as = RewardHistogram)]
pub struct Histogram {
    /// Values counted.
    pub n: usize,
    /// Smallest.
    pub min: Option<f64>,
    /// Largest.
    pub max: Option<f64>,
    /// Mean.
    pub mean: Option<f64>,
    /// Standard deviation (population).
    pub std: Option<f64>,
    /// `[low, high, count]` per bin.
    pub bins: Vec<(f64, f64, usize)>,
}

/// The histogram of `values` (non-finite values are skipped).
pub fn histogram(values: &[f64]) -> Histogram {
    let v: Vec<f64> = values.iter().copied().filter(|x| x.is_finite()).collect();
    if v.is_empty() {
        return Histogram {
            n: 0,
            min: None,
            max: None,
            mean: None,
            std: None,
            bins: vec![],
        };
    }
    let n = v.len();
    let lo = v.iter().copied().fold(f64::INFINITY, f64::min);
    let hi = v.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let mean = v.iter().map(|x| x / n as f64).sum::<f64>();
    let var = v.iter().map(|x| (x - mean).powi(2) / n as f64).sum::<f64>();
    let span = hi - lo;
    let bins = if span == 0.0 || !span.is_finite() {
        vec![(lo, hi, n)]
    } else {
        let width = span / BINS as f64;
        let mut counts = vec![0usize; BINS];
        for x in &v {
            let k = (((x - lo) / width) as usize).min(BINS - 1);
            counts[k] += 1;
        }
        counts
            .into_iter()
            .enumerate()
            .map(|(k, c)| (lo + width * k as f64, lo + width * (k + 1) as f64, c))
            .collect()
    };
    Histogram {
        n,
        min: Some(lo),
        max: Some(hi),
        mean: Some(mean),
        std: Some(var.sqrt()),
        bins,
    }
}

/// Average ranks (ties share their mean rank).
fn ranks(v: &[f64]) -> Vec<f64> {
    let mut idx: Vec<usize> = (0..v.len()).collect();
    idx.sort_by(|a, b| v[*a].total_cmp(&v[*b]));
    let mut out = vec![0.0; v.len()];
    let mut i = 0;
    while i < idx.len() {
        let mut j = i;
        while j + 1 < idx.len() && v[idx[j + 1]] == v[idx[i]] {
            j += 1;
        }
        let r = (i + j) as f64 / 2.0 + 1.0;
        for k in &idx[i..=j] {
            out[*k] = r;
        }
        i = j + 1;
    }
    out
}

/// Spearman's rank correlation of the pairs; `None` with fewer than 3 pairs or no spread.
pub fn spearman(pairs: &[(f64, f64)]) -> Option<f64> {
    let pairs: Vec<(f64, f64)> = pairs
        .iter()
        .copied()
        .filter(|(a, b)| a.is_finite() && b.is_finite())
        .collect();
    if pairs.len() < 3 {
        return None;
    }
    let ra = ranks(&pairs.iter().map(|p| p.0).collect::<Vec<_>>());
    let rb = ranks(&pairs.iter().map(|p| p.1).collect::<Vec<_>>());
    let n = ra.len() as f64;
    let (ma, mb) = (ra.iter().sum::<f64>() / n, rb.iter().sum::<f64>() / n);
    let (mut cov, mut va, mut vb) = (0.0, 0.0, 0.0);
    for (a, b) in ra.iter().zip(&rb) {
        cov += (a - ma) * (b - mb);
        va += (a - ma).powi(2);
        vb += (b - mb).powi(2);
    }
    (va > 0.0 && vb > 0.0).then(|| (cov / (va.sqrt() * vb.sqrt())).clamp(-1.0, 1.0))
}

fn percentile(sorted: &[f64], p: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let k = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted.get(k).copied()
}

/// The learnability check.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[schema(as = RewardLearnability)]
pub struct Learnability {
    /// Prompt groups with at least two scored completions.
    pub groups: usize,
    /// Groups whose totals are all equal.
    pub zero_spread_groups: usize,
    /// `zero_spread_groups / groups` (`null` without groups).
    pub zero_spread_share: Option<f64>,
    /// True above the warning share.
    pub warning: bool,
    /// What it means, when it warns.
    pub message: Option<String>,
}

/// Groups of `rows` by `group_id` (scored totals only).
pub fn learnability(rows: &[&Row], warn_share: f64) -> Learnability {
    let mut groups: BTreeMap<&str, Vec<f64>> = BTreeMap::new();
    for r in rows {
        if let Some(t) = r.total.filter(|t| t.is_finite()) {
            groups.entry(r.group_id.as_str()).or_default().push(t);
        }
    }
    let sized: Vec<&Vec<f64>> = groups.values().filter(|g| g.len() >= 2).collect();
    let zero = sized
        .iter()
        .filter(|g| g.iter().all(|x| (x - g[0]).abs() <= 1e-12))
        .count();
    let share = (!sized.is_empty()).then(|| zero as f64 / sized.len() as f64);
    let warning = share.is_some_and(|s| s > warn_share);
    Learnability {
        groups: sized.len(),
        zero_spread_groups: zero,
        zero_spread_share: share,
        warning,
        message: warning.then(|| {
            format!(
                "In {zero} of {} prompt groups every answer got the same reward. Group-based methods like GRPO learn nothing from those groups.",
                sized.len()
            )
        }),
    }
}

/// Statistics of one version's rows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[schema(as = RewardSampleStats)]
pub struct Summary {
    /// `sampled`: computed from a preview sample, not from training.
    pub tag: String,
    /// Samples.
    pub samples: usize,
    /// Share of samples with a failed component (status other than OK / NOT_APPLICABLE).
    pub failure_rate: f64,
    /// Share with a TIMEOUT.
    pub timeout_rate: f64,
    /// Share MASKED.
    pub masked_rate: f64,
    /// Share FALLBACK_APPLIED.
    pub fallback_rate: f64,
    /// Share stopped by a gate.
    pub gate_hit_rate: f64,
    /// The total.
    pub total: Histogram,
    /// Each component.
    pub components: BTreeMap<String, Histogram>,
    /// Median scoring time per sample (ms).
    pub latency_p50_ms: Option<f64>,
    /// 95th percentile (ms).
    pub latency_p95_ms: Option<f64>,
    /// Spearman's ρ between total and completion length.
    pub length_spearman: Option<f64>,
    /// The learnability check.
    pub learnability: Learnability,
}

/// The summary of `rows` (one version's).
pub fn summarize(rows: &[&Row], warn_share: f64) -> Summary {
    let n = rows.len().max(1) as f64;
    let share = |f: &dyn Fn(&Row) -> bool| rows.iter().filter(|r| f(r)).count() as f64 / n;
    let totals: Vec<f64> = rows.iter().filter_map(|r| r.total).collect();
    let mut by_comp: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    for r in rows {
        for (k, v) in &r.components {
            by_comp
                .entry(k.clone())
                .or_default()
                .extend(v.iter().copied());
        }
    }
    let mut lat: Vec<f64> = rows
        .iter()
        .map(|r| r.latency_ms)
        .filter(|x| x.is_finite())
        .collect();
    lat.sort_by(f64::total_cmp);
    let length: Vec<(f64, f64)> = rows
        .iter()
        .filter_map(|r| r.total.map(|t| (t, f64::from(r.completion_tokens))))
        .collect();
    Summary {
        tag: "sampled".into(),
        samples: rows.len(),
        failure_rate: share(&|r| !matches!(r.status.as_str(), "OK" | "NOT_APPLICABLE" | "")),
        timeout_rate: share(&|r| r.status == "TIMEOUT"),
        masked_rate: share(&|r| r.resolution == "MASKED"),
        fallback_rate: share(&|r| r.resolution == "FALLBACK_APPLIED"),
        gate_hit_rate: share(&|r| r.flags.iter().any(|f| f == "gated")),
        total: histogram(&totals),
        components: by_comp
            .into_iter()
            .map(|(k, v)| (k, histogram(&v)))
            .collect(),
        latency_p50_ms: percentile(&lat, 0.5),
        latency_p95_ms: percentile(&lat, 0.95),
        length_spearman: spearman(&length),
        learnability: learnability(rows, warn_share),
    }
}

/// Two versions on the same samples.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[schema(as = RewardComparisonStats)]
pub struct Comparison {
    /// Samples both versions scored.
    pub paired: usize,
    /// Share of paired samples where both totals are equal (within 1e-9).
    pub agreement: Option<f64>,
    /// Spearman's ρ between the two versions' totals.
    pub spearman: Option<f64>,
    /// Mean of `b − a`.
    pub mean_delta: Option<f64>,
    /// `[a, b]` per paired sample (at most 1000).
    pub scatter: Vec<(f64, f64)>,
}

/// Pairs the rows of version `a` and `b` by sample number.
pub fn compare(rows: &[Row]) -> Comparison {
    let mut a: BTreeMap<u64, f64> = BTreeMap::new();
    let mut b: BTreeMap<u64, f64> = BTreeMap::new();
    for r in rows {
        if let Some(t) = r.total.filter(|t| t.is_finite()) {
            match r.version.as_str() {
                "b" => b.insert(r.i, t),
                _ => a.insert(r.i, t),
            };
        }
    }
    let pairs: Vec<(f64, f64)> = a
        .iter()
        .filter_map(|(i, x)| b.get(i).map(|y| (*x, *y)))
        .collect();
    let n = pairs.len();
    let same = pairs.iter().filter(|(x, y)| (x - y).abs() <= 1e-9).count();
    Comparison {
        paired: n,
        agreement: (n > 0).then(|| same as f64 / n as f64),
        spearman: spearman(&pairs),
        mean_delta: (n > 0).then(|| pairs.iter().map(|(x, y)| (y - x) / n as f64).sum()),
        scatter: pairs.into_iter().take(MAX_SCATTER).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(i: u64, group: &str, total: Option<f64>, version: &str) -> Row {
        Row {
            i,
            group_id: group.into(),
            version: version.into(),
            total,
            resolution: if total.is_some() { "SCORED" } else { "MASKED" }.into(),
            status: "OK".into(),
            components: BTreeMap::new(),
            flags: vec![],
            completion_tokens: (i as u32) * 10,
            latency_ms: i as f64,
        }
    }

    #[test]
    fn spearman_matches_known_values() {
        let close = |got: Option<f64>, want: f64| got.is_some_and(|g| (g - want).abs() < 1e-12);
        assert!(close(spearman(&[(1.0, 2.0), (2.0, 4.0), (3.0, 9.0)]), 1.0));
        assert!(close(spearman(&[(1.0, 3.0), (2.0, 2.0), (3.0, 1.0)]), -1.0));
        // Ties take their average rank: x ranks 1.5,1.5,3 and y 1,2,3 → ρ = √3/2.
        let r = spearman(&[(1.0, 1.0), (1.0, 2.0), (2.0, 3.0)]).unwrap();
        assert!((r - 0.866_025_403_784_438_6).abs() < 1e-12, "{r}");
        assert_eq!(spearman(&[(1.0, 1.0), (1.0, 2.0), (1.0, 3.0)]), None);
        assert_eq!(spearman(&[(1.0, 1.0)]), None);
    }

    #[test]
    fn histogram_counts_every_value_once() {
        let h = histogram(&[0.0, 0.5, 1.0, 1.0, f64::NAN]);
        assert_eq!(h.n, 4);
        assert_eq!(h.bins.iter().map(|b| b.2).sum::<usize>(), 4);
        assert_eq!(h.bins.len(), BINS);
        assert_eq!(h.mean, Some(0.625));
        let flat = histogram(&[2.0, 2.0]);
        assert_eq!(flat.bins, vec![(2.0, 2.0, 2)]);
        assert_eq!(histogram(&[]).n, 0);
    }

    #[test]
    fn a_constant_reward_trips_the_learnability_check() {
        let rows: Vec<Row> = (0..8)
            .map(|i| row(i, &format!("g{}", i / 4), Some(1.0), "a"))
            .collect();
        let refs: Vec<&Row> = rows.iter().collect();
        let l = learnability(&refs, 0.9);
        assert_eq!((l.groups, l.zero_spread_groups, l.warning), (2, 2, true));
        assert!(l.message.unwrap().contains("GRPO"));
        let mut varied = rows.clone();
        varied[0].total = Some(0.0);
        varied[4].total = Some(0.0);
        let refs: Vec<&Row> = varied.iter().collect();
        assert!(!learnability(&refs, 0.9).warning);
    }

    #[test]
    fn summary_rates_and_comparison_pairs_by_sample() {
        let mut rows = vec![
            row(1, "g", Some(1.0), "a"),
            row(2, "g", None, "a"),
            row(3, "h", Some(0.5), "a"),
        ];
        rows[1].status = "TIMEOUT".into();
        rows[2].flags = vec!["gated".into()];
        let refs: Vec<&Row> = rows.iter().collect();
        let s = summarize(&refs, 0.9);
        assert!((s.timeout_rate - 1.0 / 3.0).abs() < 1e-12);
        assert!((s.masked_rate - 1.0 / 3.0).abs() < 1e-12);
        assert!((s.gate_hit_rate - 1.0 / 3.0).abs() < 1e-12);
        assert_eq!(s.tag, "sampled");
        let mut both = rows.clone();
        both.extend([row(1, "g", Some(1.0), "b"), row(3, "h", Some(0.0), "b")]);
        let c = compare(&both);
        assert_eq!(c.paired, 2);
        assert_eq!(c.agreement, Some(0.5));
        assert_eq!(c.mean_delta, Some(-0.25));
    }
}
