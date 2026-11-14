#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["pyyaml==6.0.3", "jsonschema==4.26.0"]
#
# [tool.uv]
# exclude-newer = "2026-11-01T00:00:00Z"
# ///
"""Merge guidance data + fragments, validate them, and write guidance/build/.

    uv run --script guidance/tools/build.py          # write build outputs
    uv run --script guidance/tools/build.py --check  # validate; fail if outputs are stale
"""

from __future__ import annotations

import argparse
import json
import pprint
import re
import sys
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator

# data file stem -> (fragment folder or None, list keys a fragment may add to)
SECTIONS = {
    "goals": (None, ()),
    "recipes": (None, ()),
    "hardware-profiles": (None, ()),
    "glossary": ("glossary", ("terms",)),
    "errors": ("errors", ("errors",)),
    "requirements": ("requirements", ("components", "requirements")),
}
FRAGMENT_NAME = re.compile(r"^p\d{2}\.yaml$")

# Keys whose string (or list-of-string) values are ids, enums or links, not user-facing text.
NON_TEXT_KEYS = {
    "id", "code", "aliases", "learn_more", "docs_link", "profile", "platform", "task", "family",
    "method", "peft", "precision", "dataset_schema", "reward_type", "recipes", "components",
    "support", "memory_kind",
}
MAX_WORDS = 20
SENTENCE_END = re.compile(r"(?<=[.!?])\s+")
CODE_SPAN = re.compile(r"`[^`]*`")
ACRONYM = re.compile(r"\b(?=[A-Z0-9]*[A-Z][A-Z0-9]*[A-Z])[A-Z0-9]{2,}\b")
INVALID = re.compile(r"\binvalid\b", re.IGNORECASE)
BECAUSE = re.compile(r"\bbecause\b", re.IGNORECASE)
BLAME = re.compile(
    r"\b(your fault|you failed|you forgot|user error|you should have|wrong input|bad input)\b",
    re.IGNORECASE,
)


def sentences(text: str) -> list[str]:
    return [s for s in SENTENCE_END.split(text.strip()) if s]


def style_problems(text: str, known_acronyms: set[str]) -> list[str]:
    """Writing rules from guidance/copy-style.md that a machine can check."""
    problems = []
    plain = CODE_SPAN.sub("CMD", text)  # a command counts as one word and is not prose
    for sentence in sentences(plain):
        words = len(sentence.split())
        if words > MAX_WORDS:
            problems.append(f"sentence has {words} words (max {MAX_WORDS}): {sentence!r}")
        if INVALID.search(sentence) and not BECAUSE.search(sentence):
            problems.append(f'say what is wrong instead of a bare "invalid": {sentence!r}')
        if match := BLAME.search(sentence):
            problems.append(f"don't blame the user ({match.group(0)!r}): {sentence!r}")
    for acronym in sorted(set(ACRONYM.findall(plain)) - known_acronyms - {"CMD"}):
        problems.append(f"acronym {acronym!r} is not in the glossary")
    return problems


def texts(node, path: str):
    """Yield (path, string) for every user-facing string."""
    if isinstance(node, dict):
        for key, value in node.items():
            is_ids = isinstance(value, str) or (isinstance(value, list) and all(isinstance(v, str) for v in value))
            if not (key in NON_TEXT_KEYS and is_ids):
                yield from texts(value, f"{path}.{key}")
    elif isinstance(node, list):
        for i, value in enumerate(node):
            yield from texts(value, f"{path}[{i}]")
    elif isinstance(node, str):
        yield path, node


def load_yaml(path: Path, validator: Draft202012Validator, problems: list[str], rel) -> dict:
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        problems.append(f"{rel(path)}: can't read YAML: {exc}")
        return {}
    for err in sorted(validator.iter_errors(doc), key=lambda e: list(e.absolute_path)):
        where = "/".join(str(p) for p in err.absolute_path) or "(top)"
        problems.append(f"{rel(path)}: {where}: {err.message}")
    return doc if isinstance(doc, dict) else {}


def collect(root: Path, problems: list[str]) -> dict[str, list[tuple[str, dict]]]:
    """Load data + fragments. Returns section -> list of (source, document)."""

    def rel(path: Path) -> str:
        return path.relative_to(root.parent).as_posix()

    docs: dict[str, list[tuple[str, dict]]] = {}
    for stem, (folder, fragment_keys) in SECTIONS.items():
        schema = json.loads((root / "schemas" / f"{stem}.schema.json").read_text(encoding="utf-8"))
        validator = Draft202012Validator(schema)
        base = root / "data" / f"{stem}.yaml"
        docs[stem] = [(rel(base), load_yaml(base, validator, problems, rel))]
        if folder is None:
            continue
        for path in sorted((root / "fragments" / folder).iterdir()):
            if path.name == "README.md":
                continue
            if not FRAGMENT_NAME.match(path.name):
                problems.append(f"{rel(path)}: fragment files must be named pNN.yaml")
                continue
            doc = load_yaml(path, validator, problems, rel)
            for key in set(doc) - set(fragment_keys):
                problems.append(f"{rel(path)}: fragments can't set {key!r}, only {list(fragment_keys)}")
            docs[stem].append((rel(path), doc))
    return docs


def merged(docs: list[tuple[str, dict]], key: str) -> list[tuple[str, dict]]:
    return [(src, item) for src, doc in docs for item in doc.get(key, [])]


def unique(items, key_of, what: str, problems: list[str]) -> None:
    seen: dict = {}
    for src, item in items:
        for key in key_of(item):
            if key in seen:
                problems.append(f"{src}: duplicate {what} {key!r} (first in {seen[key]})")
            else:
                seen[key] = src


def build(root: Path) -> tuple[dict, list[str]]:
    problems: list[str] = []
    docs = collect(root, problems)
    if problems:  # don't cross-check documents that failed their schema
        return {}, problems

    base = {stem: entries[0][1] for stem, entries in docs.items()}
    goals = merged(docs["goals"], "goals")
    recipes = merged(docs["recipes"], "recipes")
    families = merged(docs["hardware-profiles"], "families")
    profiles = merged(docs["hardware-profiles"], "profiles")
    terms = merged(docs["glossary"], "terms")
    errors = merged(docs["errors"], "errors")
    components = merged(docs["requirements"], "components")
    platforms = merged(docs["requirements"], "platforms")
    rows = merged(docs["requirements"], "requirements")

    if "fallback" not in base["errors"]:
        problems.append("guidance/data/errors.yaml: needs a 'fallback' entry")

    # Duplicates
    by_id = lambda item: [item["id"]]  # noqa: E731
    for items, what in [(goals, "goal"), (recipes, "recipe"), (families, "family"),
                        (profiles, "profile"), (components, "component"), (platforms, "platform")]:
        unique(items, by_id, f"{what} id", problems)
    unique(errors, lambda e: [e["code"]], "error code", problems)
    unique(terms, lambda t: [n.lower() for n in [t["term"], *t.get("aliases", [])]], "glossary term", problems)
    unique(rows, lambda r: [(r["task"], r["platform"])], "requirement", problems)

    # Cross-references
    recipe_ids = {r["id"] for _, r in recipes}
    family_ids = {f["id"] for _, f in families}
    profile_ids = {p["id"] for _, p in profiles}
    platform_ids = {p["id"] for _, p in platforms}
    component_by_id = {c["id"]: c for _, c in components}
    error_codes = {e["code"] for _, e in errors}
    for src, goal in goals:
        for rid in goal["recipes"]:
            if rid not in recipe_ids:
                problems.append(f"{src}: goal {goal['id']!r} names unknown recipe {rid!r}")
    for src, profile in profiles:
        if profile["family"] not in family_ids:
            problems.append(f"{src}: profile {profile['id']!r} has unknown family {profile['family']!r}")
    for src, recipe in recipes:
        unique([(src, p) for p in recipe["profiles"]], lambda p: [p["profile"]],
               f"profile in recipe {recipe['id']!r}", problems)
        for p in recipe["profiles"]:
            if p["profile"] not in profile_ids:
                problems.append(f"{src}: recipe {recipe['id']!r} names unknown profile {p['profile']!r}")
            for name in ("model_params_b", "minutes_per_1k_samples"):
                if p[name]["min"] > p[name]["max"]:
                    problems.append(f"{src}: recipe {recipe['id']!r} {p['profile']}: {name} min > max")
    for src, error in errors:
        parent = error["code"].split("/")[0]
        if "/" in error["code"] and parent not in error_codes:
            problems.append(f"{src}: sub-code {error['code']!r} has no parent class {parent!r}")
    for src, comp in components:
        for platform, size in comp["download_gb"].items():
            if platform not in platform_ids:
                problems.append(f"{src}: component {comp['id']!r} names unknown platform {platform!r}")
            if size["min"] > size["max"]:
                problems.append(f"{src}: component {comp['id']!r} {platform}: min > max")
    for src, row in rows:
        label = f"{row['task']} on {row['platform']}"
        if row["platform"] not in platform_ids:
            problems.append(f"{src}: {label}: unknown platform")
        for cid in row.get("components", []):
            comp = component_by_id.get(cid)
            if comp is None:
                problems.append(f"{src}: {label}: unknown component {cid!r}")
            elif row["platform"] not in comp["download_gb"]:
                problems.append(f"{src}: {label}: component {cid!r} has no size for this platform")

    # Writing rules
    known = {n for _, t in terms for n in [t["term"], *t.get("aliases", [])]}
    for stem, entries in docs.items():
        for src, doc in entries:
            for path, text in texts(doc, stem):
                problems.extend(f"{src}: {path}: {p}" for p in style_problems(text, known))
    for src, term in terms:
        if len(sentences(term["plain"])) != 1:
            problems.append(f"{src}: glossary {term['term']!r}: plain must be exactly one sentence")

    if problems:
        return {}, problems

    def row_out(row: dict) -> dict:
        out = dict(row)
        if row["supported"]:
            sizes = [component_by_id[c]["download_gb"][row["platform"]] for c in row["components"]]
            out["download_gb"] = {
                "min": round(sum(s["min"] for s in sizes), 2),
                "max": round(sum(s["max"] for s in sizes), 2),
            }
            out["estimated"] = True
        return out

    items = lambda pairs: [item for _, item in pairs]  # noqa: E731
    catalog = {
        "schema_version": 1,
        "goals": items(goals),
        "recipes": items(recipes),
        "hardware": {"families": items(families), "profiles": items(profiles)},
        "glossary": sorted(items(terms), key=lambda t: t["term"].lower()),
        "errors": sorted(items(errors), key=lambda e: e["code"]),
        "error_fallback": base["errors"]["fallback"],
        "requirements": {
            "components": items(components),
            "platforms": items(platforms),
            "rows": sorted((row_out(r) for _, r in rows), key=lambda r: (r["task"], r["platform"])),
        },
    }
    return catalog, []


# ---------- Generated loaders ----------

HEADER = "@generated by guidance/tools/build.py from guidance/data and guidance/fragments. Do not edit."

JS = """// {header}
// Dependency-free loader for the guidance catalog.

const deepFreeze = (value) => {{
  if (value && typeof value === "object") {{
    Object.values(value).forEach(deepFreeze);
    Object.freeze(value);
  }}
  return value;
}};

export const catalog = deepFreeze({data});

const find = (list, key, value) => list.find((item) => item[key] === value);

export const getError = (code) => find(catalog.errors, "code", code);
export const explainError = (code) => getError(code) ?? catalog.error_fallback;
export const getTerm = (name) => {{
  const wanted = name.toLowerCase();
  return catalog.glossary.find((t) =>
    [t.term, ...(t.aliases ?? [])].some((n) => n.toLowerCase() === wanted),
  );
}};
export const getGoal = (id) => find(catalog.goals, "id", id);
export const getRecipe = (id) => find(catalog.recipes, "id", id);
export const getHardwareProfile = (id) => find(catalog.hardware.profiles, "id", id);
export const getRequirement = (task, platform) =>
  catalog.requirements.rows.find((r) => r.task === task && r.platform === platform);
"""

DTS = """// {header}

export interface Range {{ min: number; max: number }}
export interface Steps {{ steps: string[]; windows?: string[]; macos?: string[]; linux?: string[] }}
export interface Explanation {{ title: string; why: string; what_to_do: Steps; docs_link?: string }}
export interface ErrorEntry extends Explanation {{ code: string }}
export interface Term {{ term: string; plain: string; aliases?: string[]; learn_more?: string }}
export interface Goal {{
  id: string;
  title: string;
  summary: string;
  recipes: string[];
  dataset_schema: "chat" | "question-answer" | "preference-pairs" | "prompt-with-answer" | "prompts";
  reward_type: "none" | "preference" | "verifiable" | "teacher";
}}
export type Precision = "bf16" | "fp32" | "nf4-bf16" | "mlx-q4-bf16";
export interface RecipeProfile {{
  profile: string;
  model_params_b: Range;
  per_device_batch_size: number;
  minutes_per_1k_samples: Range;
  precision?: Precision;
}}
export interface Recipe {{
  id: string;
  name: string;
  method: "sft" | "dpo" | "grpo" | "distillation";
  summary: string;
  peft: "none" | "lora" | "qlora";
  precision: Precision;
  defaults: Record<string, number>;
  profiles: RecipeProfile[];
  estimated: true;
}}
export interface HardwareFamily {{
  id: "nvidia" | "apple-silicon" | "cpu";
  name: string;
  headroom: {{
    gpu_memory_free_pct?: number;
    system_ram_free_min_gb?: number;
    system_ram_free_pct?: number;
    unified_memory_reserved_pct?: number;
  }};
  rule: string;
}}
export interface HardwareProfile {{
  id: string;
  family: HardwareFamily["id"];
  name: string;
  memory_kind: "gpu" | "unified" | "system";
  memory_gb: {{ min: number; max?: number }};
  summary: string;
  verified: false;
}}
export interface Component {{
  id: string;
  name: string;
  summary: string;
  download_gb: Record<string, Range>;
  estimated: true;
}}
export interface Platform {{
  id: string;
  name: string;
  support: "full" | "wsl2-only" | "partial" | "tiny-models";
  summary: string;
}}
export interface Requirement {{
  task: string;
  platform: string;
  supported: boolean;
  components?: string[];
  note?: string;
  /** Sum of component downloads. Present when supported. */
  download_gb?: Range;
  estimated?: true;
}}
export interface Catalog {{
  schema_version: number;
  goals: Goal[];
  recipes: Recipe[];
  hardware: {{ families: HardwareFamily[]; profiles: HardwareProfile[] }};
  glossary: Term[];
  errors: ErrorEntry[];
  error_fallback: Explanation;
  requirements: {{ components: Component[]; platforms: Platform[]; rows: Requirement[] }};
}}

export declare const catalog: Readonly<Catalog>;
export declare function getError(code: string): ErrorEntry | undefined;
/** The entry for `code`, or the generic fallback when the code has none. */
export declare function explainError(code: string): Explanation;
/** Case-insensitive; matches aliases too. */
export declare function getTerm(name: string): Term | undefined;
export declare function getGoal(id: string): Goal | undefined;
export declare function getRecipe(id: string): Recipe | undefined;
export declare function getHardwareProfile(id: string): HardwareProfile | undefined;
export declare function getRequirement(task: string, platform: string): Requirement | undefined;
"""

PY = '''"""{header}

Dependency-free (stdlib only) loader for the guidance catalog.
"""

from typing import Any

CATALOG: dict[str, Any] = {data}


def _find(items: list[dict[str, Any]], key: str, value: str) -> dict[str, Any] | None:
    return next((item for item in items if item[key] == value), None)


def get_error(code: str) -> dict[str, Any] | None:
    return _find(CATALOG["errors"], "code", code)


def explain_error(code: str) -> dict[str, Any]:
    """The entry for `code`, or the generic fallback when the code has none."""
    return get_error(code) or CATALOG["error_fallback"]


def get_term(name: str) -> dict[str, Any] | None:
    """Case-insensitive; matches aliases too."""
    wanted = name.lower()
    return next(
        (t for t in CATALOG["glossary"] if wanted in {{n.lower() for n in [t["term"], *t.get("aliases", [])]}}),
        None,
    )


def get_goal(goal_id: str) -> dict[str, Any] | None:
    return _find(CATALOG["goals"], "id", goal_id)


def get_recipe(recipe_id: str) -> dict[str, Any] | None:
    return _find(CATALOG["recipes"], "id", recipe_id)


def get_hardware_profile(profile_id: str) -> dict[str, Any] | None:
    return _find(CATALOG["hardware"]["profiles"], "id", profile_id)


def get_requirement(task: str, platform: str) -> dict[str, Any] | None:
    rows = CATALOG["requirements"]["rows"]
    return next((r for r in rows if r["task"] == task and r["platform"] == platform), None)
'''

RS = """//! {header}
//!
//! Dependency-free loader for the guidance catalog: the whole catalog as static data.
#![allow(dead_code)]

/// A catalog value, like a JSON value with static lifetime.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Value {{
    Null,
    Bool(bool),
    Num(f64),
    Str(&'static str),
    List(&'static [Value]),
    Map(&'static [(&'static str, Value)]),
}}

impl Value {{
    /// The value under `key` in a map.
    pub fn get(&self, key: &str) -> Option<&'static Value> {{
        match *self {{
            Value::Map(entries) => entries.iter().find(|(k, _)| *k == key).map(|(_, v)| v),
            _ => None,
        }}
    }}

    /// The string under `key` in a map.
    pub fn str(&self, key: &str) -> Option<&'static str> {{
        self.get(key)?.as_str()
    }}

    pub fn as_str(&self) -> Option<&'static str> {{
        match *self {{
            Value::Str(s) => Some(s),
            _ => None,
        }}
    }}

    pub fn as_f64(&self) -> Option<f64> {{
        match *self {{
            Value::Num(n) => Some(n),
            _ => None,
        }}
    }}

    pub fn as_bool(&self) -> Option<bool> {{
        match *self {{
            Value::Bool(b) => Some(b),
            _ => None,
        }}
    }}

    pub fn as_list(&self) -> &'static [Value] {{
        match *self {{
            Value::List(items) => items,
            _ => &[],
        }}
    }}
}}

pub static CATALOG: Value = {data};

fn section(path: &[&str]) -> &'static [Value] {{
    let mut node: &'static Value = &CATALOG;
    for key in path {{
        match node.get(key) {{
            Some(next) => node = next,
            None => return &[],
        }}
    }}
    node.as_list()
}}

fn find(path: &[&str], key: &str, value: &str) -> Option<&'static Value> {{
    section(path).iter().find(|item| item.str(key) == Some(value))
}}

pub fn error(code: &str) -> Option<&'static Value> {{
    find(&["errors"], "code", code)
}}

/// The entry for `code`, or the generic fallback when the code has none.
pub fn explain_error(code: &str) -> &'static Value {{
    error(code).unwrap_or_else(|| CATALOG.get("error_fallback").expect("catalog has a fallback"))
}}

/// Case-insensitive; matches aliases too.
pub fn term(name: &str) -> Option<&'static Value> {{
    section(&["glossary"]).iter().find(|t| {{
        let aliases = t.get("aliases").map_or(&[][..], Value::as_list);
        t.str("term").into_iter().chain(aliases.iter().filter_map(Value::as_str)).any(|n| n.eq_ignore_ascii_case(name))
    }})
}}

pub fn goal(id: &str) -> Option<&'static Value> {{
    find(&["goals"], "id", id)
}}

pub fn recipe(id: &str) -> Option<&'static Value> {{
    find(&["recipes"], "id", id)
}}

pub fn hardware_profile(id: &str) -> Option<&'static Value> {{
    find(&["hardware", "profiles"], "id", id)
}}

pub fn requirement(task: &str, platform: &str) -> Option<&'static Value> {{
    section(&["requirements", "rows"])
        .iter()
        .find(|r| r.str("task") == Some(task) && r.str("platform") == Some(platform))
}}
"""


def rust_value(value, indent: int = 0) -> str:
    pad = "    " * (indent + 1)
    end = "    " * indent
    if value is None:
        return "Value::Null"
    if isinstance(value, bool):
        return f"Value::Bool({str(value).lower()})"
    if isinstance(value, (int, float)):
        return f"Value::Num({float(value)!r})"
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        return f'Value::Str("{escaped}")'
    if isinstance(value, list):
        if not value:
            return "Value::List(&[])"
        inner = "".join(f"{pad}{rust_value(v, indent + 1)},\n" for v in value)
        return f"Value::List(&[\n{inner}{end}])"
    inner = "".join(f'{pad}("{k}", {rust_value(v, indent + 1)}),\n' for k, v in value.items())
    return f"Value::Map(&[\n{inner}{end}])"


def outputs(catalog: dict) -> dict[str, str]:
    as_json = json.dumps(catalog, indent=2, ensure_ascii=False)
    return {
        "catalog.json": as_json + "\n",
        "index.js": JS.format(header=HEADER, data=as_json),
        "index.d.ts": DTS.format(header=HEADER),
        "catalog.py": PY.format(header=HEADER, data=pprint.pformat(catalog, width=100, sort_dicts=False)),
        "catalog.rs": RS.format(header=HEADER, data=rust_value(catalog)),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--check", action="store_true", help="validate and fail if build outputs are stale")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1],
                        help="the guidance folder (default: the one holding this script)")
    args = parser.parse_args()
    root = args.root.resolve()

    catalog, problems = build(root)
    if problems:
        print(f"guidance: {len(problems)} problem(s):", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1

    out_dir = root / "build"
    stale = []
    for name, content in outputs(catalog).items():
        path = out_dir / name
        if args.check:
            if not path.exists() or path.read_text(encoding="utf-8") != content:
                stale.append(name)
        else:
            out_dir.mkdir(exist_ok=True)
            path.write_text(content, encoding="utf-8")
    if stale:
        print(f"guidance: build outputs are out of date: {', '.join(stale)}", file=sys.stderr)
        print("Run: uv run --script guidance/tools/build.py", file=sys.stderr)
        return 1
    counts = f"{len(catalog['errors'])} errors, {len(catalog['glossary'])} terms"
    print(f"guidance: catalog OK ({counts}){'' if args.check else ' - wrote guidance/build/'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
