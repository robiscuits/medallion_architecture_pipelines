import pandas as pd
import glob
from pathlib import Path
import os

BRONZE_PATH = "./bronze/dt=*/*.parquet"
OUT = Path("./final")
INT = Path("./intermediate")
OUT.mkdir(exist_ok=True)
INT.mkdir(exist_ok=True)


print("Current working dir:", os.getcwd())

def load_bronze():
    files = [pd.read_parquet(f) for f in glob.glob(BRONZE_PATH)]
    loaded = pd.concat(files, ignore_index=True)
    return loaded

def make_unique(cols):
    seen = {}
    new_cols = []
    for c in cols:
        if c not in seen:
            seen[c] = 1
            new_cols.append(c)
        else:
            seen[c] += 1
            new_cols.append(f"{c}__{seen[c]}")
    return new_cols

def snake_case(df):
    df = df.copy()
    df.columns = (
        df.columns
        .str.replace("([a-z0-9])([A-Z])", r"\1_\2", regex=True)
        .str.replace("pitchData", "pitch_data")
        .str.replace("hitData", "hit_data")
        .str.replace("__+", "_", regex=True)
        .str.lower()
    )
    # IMPORTANT: do NOT remove duplicates — rename them uniquely
    df.columns = make_unique(df.columns)
    return df

def build_silver1(df):
    silver1_cols = [
        # keys
        "game_pk",
        "play_id",
        "at_bat_index",
        "pitch_number",

        # context
        "about_inning",
        "about_half_inning",
        "about_is_top_inning",

        # batter
        "offense_batter_id",
        "offense_batter_bat_side_code",

        # pitch outcome classification
        "details_type_code",
        "details_type_description",
        "details_call_description",
        "details_event",
        "details_is_strike",
        "details_is_ball",
        "details_is_in_play",

        # pitch physics
        "pitch_data_start_speed",
        "pitch_data_end_speed",
        "pitch_data_breaks_spin_rate",
        "pitch_data_breaks_break_horizontal",
        "pitch_data_breaks_break_angle",
        "pitch_data_breaks_break_length",
        "pitch_data_breaks_break_vertical_induced",
        "pitch_data_zone",
        "pitch_data_coordinates_p_x",
        "pitch_data_coordinates_p_z",
        "pitch_data_coordinates_pfx_x",
        "pitch_data_coordinates_pfx_z",
        "pitch_data_strike_zone_bottom",
        "pitch_data_strike_zone_top",

        # batted ball
        "hit_data_launch_speed",
        "hit_data_launch_angle",
        "hit_data_total_distance",
        "hit_data_location",
    ]

    cols = [c for c in silver1_cols if c in df.columns]
    core = df[cols].copy()

    # robust sorting
    sort_cols = ["game_pk"]
    if "at_bat_index" in core.columns:
        sort_cols.append("at_bat_index")
    if "pitch_number" in core.columns:
        sort_cols.append("pitch_number")

    core.sort_values(sort_cols, inplace=True)

    # artificial key
    core["pitch_id"] = (
        core["game_pk"].astype(str)
        + "-" + core.get("at_bat_index", pd.Series(["NA"] * len(core))).astype(str)
        + "-" + core.get("pitch_number", pd.Series(["NA"] * len(core))).astype(str)
    )
    return core

def build_silver2_pitching(df):
    df = df.copy()
    
    df["is_swing"] = df["details_call_description"].str.contains("swing", case=False, na=False)
    df["is_whiff"] = df["details_call_description"].str.contains("swinging strike", case=False, na=False)
    df["is_contact"] = df["details_is_in_play"] == True
    df["is_called_strike"] = df["details_call_description"].str.contains("called strike", case=False, na=False)

    horiz_inzone = df["pitch_data_coordinates_p_x"].astype(float).abs() <= 0.83
    vert_inzone = (
        (df["pitch_data_coordinates_p_z"].astype(float) >= df["pitch_data_strike_zone_bottom"].astype(float)) &
        (df["pitch_data_coordinates_p_z"].astype(float) <= df["pitch_data_strike_zone_top"].astype(float))
    )

    df["is_chase"] = (~(horiz_inzone & vert_inzone)) & df["is_swing"]
    return df

def build_silver2_batting(df):
    df = df.copy()

    df["is_swing"] = df["details_call_description"].str.contains("swing", case=False, na=False)
    df["is_contact"] = df["details_is_in_play"] == True
    df["is_take"] = ~df["is_swing"]

    df["is_strike"] = df["details_is_strike"]
    df["is_ball"] = df["details_is_ball"]

    horiz_inzone = df["pitch_data_coordinates_p_x"].astype(float).abs() <= 0.83
    vert_inzone = (
        (df["pitch_data_coordinates_p_z"].astype(float) >= df["pitch_data_strike_zone_bottom"].astype(float)) &
        (df["pitch_data_coordinates_p_z"].astype(float) <= df["pitch_data_strike_zone_top"].astype(float))
    )

    in_zone = horiz_inzone & vert_inzone
    df["is_chase"] = (~in_zone) & df["is_swing"]
    df["is_zone_swing"] = in_zone & df["is_swing"]
    df["is_zone_contact"] = in_zone & df["is_contact"]
    df["is_o_contact"] = (~in_zone) & df["is_contact"]

    return df

### MAIN ###
if __name__ == "__main__":
    bronze = load_bronze()
    bronze = snake_case(bronze)

    print("Loaded bronze:", bronze.shape)
    print("Total columns after snake_case:", len(bronze.columns))

    silver1 = build_silver1(bronze)
    silver1_path = OUT / "silver_core.parquet"
    silver1.to_parquet(silver1_path)
    print("Saved Silver1:", silver1_path)

    silver_pitch = build_silver2_pitching(silver1)
    silver_pitch_path = INT / "silver_pitching.parquet"
    silver_pitch.to_parquet(silver_pitch_path)
    print("Saved Silver2 Pitching:", silver_pitch_path)

    silver_bat = build_silver2_batting(silver1)
    silver_bat_path = INT / "silver_batting.parquet"
    silver_bat.to_parquet(silver_bat_path)
    print("Saved Silver2 Batting:", silver_bat_path)

    print("Silver pipelines complete.")
