"""Model capability inference + task mapping (v4 model finder, D1).

Three layers, most authoritative first:
1. CURATED — known model families and variant markers (chat/vision/
   deep-think/coding defaults per family)
2. NAME — variant tokens in the repo/file name (vl, vision, omni,
   coder, r1, think, reason...) when no family matched
3. META — source metadata (HF tags, pipeline_tag) when available

Every capability carries EVIDENCE ("curated:qwen", "name:vl",
"tag:vision", "default") so the console can badge curated vs inferred
honestly — capability metadata on GGUF repos is inconsistent by
publisher (recon 2026-09-20).
"""
from __future__ import annotations

import re
from typing import Any, Iterable

# ── vocabulary ──────────────────────────────────────────────────────────────

CAP_CHAT = "chat"
CAP_VISION = "vision"
CAP_DEEP_THINK = "deep_think"
CAP_CODING = "coding"
ALL_CAPS = (CAP_CHAT, CAP_VISION, CAP_DEEP_THINK, CAP_CODING)

# The console task chooser — what the user wants to DO with the model.
# "business" (requirements review / tech solution recommendation /
# business plan) is not a model capability: it maps to strong general
# models with reasoning, i.e. chat AND deep_think.
TASKS: list[dict[str, Any]] = [
    {"key": "chat", "label": "Chat",
     "capabilities": [CAP_CHAT]},
    {"key": "vision", "label": "Vision — images / video",
     "capabilities": [CAP_VISION]},
    {"key": "deep_think", "label": "Deep think — reasoning",
     "capabilities": [CAP_DEEP_THINK]},
    {"key": "coding", "label": "Code development",
     "capabilities": [CAP_CODING]},
    {"key": "business",
     "label": "Business — requirements review, tech solution, "
              "business plan",
     "capabilities": [CAP_CHAT, CAP_DEEP_THINK]},
]


def task_match(caps: Iterable[str], task_key: str) -> bool:
    """A model satisfies a task when it has ALL the task's required
    capabilities (business = general + reasoning, not just either)."""
    task = next((t for t in TASKS if t["key"] == task_key), None)
    if task is None:
        return True
    have = set(caps)
    return all(c in have for c in task["capabilities"])


# ── layer 1: curated families + variant markers ─────────────────────────────

_FAMILIES: dict[str, list[str]] = {
    "qwen": [CAP_CHAT, CAP_DEEP_THINK],
    "glm": [CAP_CHAT, CAP_DEEP_THINK],
    "deepseek": [CAP_CHAT, CAP_DEEP_THINK, CAP_CODING],
    "kimi": [CAP_CHAT, CAP_DEEP_THINK],
    "llama": [CAP_CHAT],
    "gemma": [CAP_CHAT],
    "mistral": [CAP_CHAT],
    "phi": [CAP_CHAT],
    "granite": [CAP_CHAT],
}

_VARIANTS: dict[str, list[str]] = {
    "vl": [CAP_VISION],
    "vision": [CAP_VISION],
    "omni": [CAP_VISION],
    "internvl": [CAP_VISION],
    "coder": [CAP_CODING],
    "codestral": [CAP_CODING],
    "code": [CAP_CODING],
    "r1": [CAP_DEEP_THINK],
    "think": [CAP_DEEP_THINK],
    "reason": [CAP_DEEP_THINK],
    "reasoning": [CAP_DEEP_THINK],
}


def _tokens(name: str) -> list[str]:
    return [t for t in re.split(r"[^a-z0-9]+", name.lower()) if t]


# ── layer 3: source metadata ────────────────────────────────────────────────

_TAG_MAP: dict[str, list[str]] = {
    "vision": [CAP_VISION],
    "multimodal": [CAP_VISION],
    "mmproj": [CAP_VISION],
    "any-vision": [CAP_VISION],
    "image-text-to-text": [CAP_VISION],
    "conversational": [CAP_CHAT],
    "chat": [CAP_CHAT],
    "reasoning": [CAP_DEEP_THINK],
    "thinking": [CAP_DEEP_THINK],
    "efficient-thinking": [CAP_DEEP_THINK],
    "code": [CAP_CODING],
    "coding": [CAP_CODING],
}


def infer(name: str, *, tags: Iterable[str] = (),
          pipeline_tag: str | None = None) -> dict[str, Any]:
    """Capabilities for a model/repo name (+optional source metadata).
    Returns {capabilities: [...], evidence: {cap: "curated:qwen" |
    "name:vl" | "tag:vision" | "default"}}."""
    caps: dict[str, str] = {}          # cap -> evidence
    toks = _tokens(name)

    # families are prefix-matched: qwen3/qwen3.8 -> qwen, glm53 -> glm
    family = next((fam for t in toks for fam in _FAMILIES
                   if t.startswith(fam)), None)
    if family:
        for cap in _FAMILIES[family]:
            caps[cap] = f"curated:{family}"
    # variant markers double as the NAME layer when no family matched
    prefix = "curated" if family else "name"
    for t in toks:
        for cap in _VARIANTS.get(t, ()):
            caps.setdefault(cap, f"{prefix}:{t}")

    for tag in list(tags) + ([pipeline_tag] if pipeline_tag else []):
        for cap in _TAG_MAP.get(str(tag).lower().strip(), ()):
            caps.setdefault(cap, f"tag:{tag}")

    if CAP_CHAT not in caps:
        caps[CAP_CHAT] = "default"      # every text GGUF chats
    return {"capabilities": [c for c in ALL_CAPS if c in caps],
            "evidence": caps}


# ── GGUF filename parsing ───────────────────────────────────────────────────

# UD-/GSQ-RCO- are quantization-method prefixes; the core token is the
# llama.cpp quant scheme (IQ4_XS, Q4_K_M, Q8_0, TQ1_0, MXFP4, F16...).
_QUANT_RE = re.compile(
    r"(?:ud-|gsq(?:-rco)?-)?"
    r"(?:iq\d+(?:_[a-z0-9]+)*|q\d+(?:_[a-z0-9]+)+|q\d+|tq\d(?:_\d)?|"
    r"mxfp\d|bf16|f16|f32)",
    re.IGNORECASE)

_AUX_SUFFIXES = ("-mtp.gguf", "-sparse.gguf")


def is_mmproj(filename: str) -> bool:
    """Vision-projector file — served via --mmproj, not -m."""
    return "mmproj" in filename.lower()


def is_pickable(filename: str) -> bool:
    """A MAIN model quant a user can serve (not mmproj, not speculative
    -mtp/-sparse side files, not imatrix calibration data)."""
    low = filename.lower()
    if not low.endswith(".gguf"):
        return False
    if is_mmproj(filename) or "imatrix" in low:
        return False
    return not low.endswith(_AUX_SUFFIXES)


def parse_quant(filename: str) -> str | None:
    """The quant token from a GGUF filename ('UD-Q4_K_XL', 'IQ2_S',
    'Q4_K_M', 'F16'...). None when no quant-looking token exists."""
    stem = filename.lower()
    if stem.endswith(".gguf"):
        stem = stem[:-5]
    if stem.endswith("-mtp") or stem.endswith("-sparse"):
        stem = stem.rsplit("-", 1)[0]
    matches = _QUANT_RE.findall(stem)
    return matches[-1].upper() if matches else None
