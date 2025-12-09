import pandas as pd
from pathlib import Path
from datetime import datetime
import json

SILVER2_PATH = "./intermediate/silver_pitching.parquet"
OUT = Path("./final")
OUT.mkdir(exist_ok=True)

LOG = Path("./log")
LOG.mkdir(exist_ok=True)

REGISTRY = LOG / "registry.json"
GOLD_PATH = OUT / "gold_pitching.parquet"


def load_registry():
    if REGISTRY.exists():
        return json.loads(REGISTRY.read_text())
    return {}


def load_silver2():
    return pd.read_parquet(SILVER2_PATH)


def validate_gold_table(silver2_df):
    """
    Perform lightweight validation before publishing to gold.
    We check:
        - non-empty df
        - required columns exist
        - numeric columns contain usable values
        - warn if schema drift occurs vs previous gold table
    """
    df = silver2_df.copy()

    if df.empty:
        raise ValueError("Silver2 pitching table is empty. Cannot publish gold.")

    required = [
        "game_pk",
        "pitch_id",
        "details_call_description",
        "is_swing",
        "is_whiff",
        "is_contact",
        "is_called_strike",
        "is_chase",
    ]

    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"sMissing required columns: {missing}")

    numeric_cols = ["is_swing", "is_whiff", "is_contact", "is_chase"]
    for col in numeric_cols:
        if df[col].isna().all():
            print(f"WARNING: '{col}' does not contain values.")

    # Warn on schema drift
    if GOLD_PATH.exists():
        old_df = pd.read_parquet(GOLD_PATH)
        old_cols = set(old_df.columns)
        new_cols = set(df.columns)

        if old_cols != new_cols:
            print("WARNING: Schema mismatch between existing gold and new output:")
            print("Missing from new schema:", old_cols - new_cols)
            print("New columns:", new_cols - old_cols)

    return df


def publish_gold_pitching(silver2_validated):
    # Gold is identical to silver2 for now
    return silver2_validated.copy()


def save_registry(df, version):
    timestamp = datetime.now().isoformat(timespec="seconds")

    entry = {
        "version": version,
        "timestamp": timestamp,
        "row_count": len(df),
        "columns": list(df.columns)
    }

    registry = load_registry()
    registry["gold_pitching"] = entry

    REGISTRY.write_text(json.dumps(registry, indent=4))
    print("Updated registry:", REGISTRY)


### MAIN ###
if __name__ == "__main__":

    silver2 = load_silver2()
    print("Loaded Silver2 Pitching:", silver2.shape)

    silver2_validated = validate_gold_table(silver2)
    print("Validated Silver2 Pitching")

    gold_df = publish_gold_pitching(silver2_validated)

    # Increment version
    registry = load_registry()
    old_version = registry.get("gold_pitching", {}).get("version", 0)
    new_version = old_version + 1

    # Save gold table
    gold_df.to_parquet(GOLD_PATH)
    print(f"Published Gold Pitching to {GOLD_PATH} (version {new_version})")

    # Save registry metadata
    save_registry(gold_df, new_version)

    print("Gold Pitching pipeline complete.")

