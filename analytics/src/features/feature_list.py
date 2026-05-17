import json
from pathlib import Path
from src.features.build_features import FEATURE_COLS

FEATURE_LIST = FEATURE_COLS

def export_features(path: str = "notebooks/outputs/exports/feature_list.json"):
    Path(path).write_text(json.dumps({"features": FEATURE_LIST}, indent=2))
