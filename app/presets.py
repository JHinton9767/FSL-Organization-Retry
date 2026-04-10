from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

from app.io_utils import ROOT, safe_slug


PRESET_DIR = ROOT / "config" / "analysis_presets"


def list_presets() -> List[str]:
    PRESET_DIR.mkdir(parents=True, exist_ok=True)
    return sorted(path.stem for path in PRESET_DIR.glob("*.json"))


def load_preset(name: str) -> Dict[str, object]:
    path = PRESET_DIR / f"{safe_slug(name)}.json"
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_preset(name: str, payload: Dict[str, object]) -> Path:
    PRESET_DIR.mkdir(parents=True, exist_ok=True)
    path = PRESET_DIR / f"{safe_slug(name)}.json"
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return path
