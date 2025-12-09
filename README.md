# Data Engineering Technical Assessment

This project implements a small medallion-style data pipeline (Bronze > Silver (1 and 2) > Gold) on top of club-provided payloads. 
The goal is to deliver two *gold-layer* datasets and one *silver-layer* dataset using pandas.

The bronze layer was provided to us via a Docker image that flattens raw JSON into hive-partitioned parquet files.

---

## Project Structure

```
project
  bronze                    # Output of provided Docker image
    dt=YYYY-MM-DD/*.parquet
  pipelines
    silver.py               # silver1 + silver2 pipelines
    gold_pitching.py        # gold pitchng pipeline
    gold_batting.py         # gold batting pipeline
  intermediate/
    silver_pitching.parquet # silver 2 pitching
    silver_batting.parquet  # silver 2 batting
  log
    registry.json           # versioning registry
  final
    silver_core.parquet     # silver 1 output
    gold_pitching.parquet   # gold pitching table
    gold_batting.parquet    # gold batting table
```

---

# Silver Layer Design

The silver layer is implemented in two steps:

### Silver1: Core Pitch-Level Table

Silver1 is a cleaned, minimal pitch-level dataset containing:

- keys (`game_pk`, `play_id`, `at_bat_index`, `pitch_number`)
- contextual values (inning, half-inning, top/bottom)
- pitch outcome fields
- pitch physics data  
- batted-ball data

**Important:**  
During development we discovered that the bronze source contains _duplicate column names_ after snake-casing.  
Earlier versions of the pipeline were silently **dropping** one of the duplicates.  
This caused missing columns in downstream steps AND inconsistency based on what parquet files were loaded in what order. `at_bat_index` kept breaking Silver1 sorting.

We fixed this by replacing duplicate dropping with **deterministic renaming**, producing:

```
at_bat_index
at_bat_index__2
at_bat_index__3
...
```

This guarantees Silver1 has access to all schema variations present in bronze. Kind of like a full-outer-join rather than an inner or a left.

---

### Silver2: Feature Engineering Layer

Silver2 adds *derived features* needed for modeling:

#### Pitching Silver2
- `is_swing`
- `is_whiff`
- `is_contact`
- `is_called_strike`
- `is_chase` (zone-based)

#### Batting Silver2
- `is_swing`
- `is_take`
- `is_contact`
- `is_strike`, `is_ball`
- `is_chase`
- `is_zone_swing`
- `is_zone_contact`
- `is_o_contact`

This is also where we would do rollups if they made sense for this project. We didn't think they really did, given the time constraints and the requirements.

**Conceptual reason for Silver2:**  
Silver1 is the *lowest-granularity cleaned table*.  
Silver2 is the *feature layer*.  
Gold should **not** contain intermediate logic.  
Keeping modeling features in Silver2 ensures:

- reproducibility  
- separation of concerns  
- easier debugging  
- gold remains a “publish-only” versioning layer.

Think of these silver2s essentially as "gold-lite" tables. They should match what's in gold, but we can afford to overwrite these without worrying about breaking anything in production

---

# Gold Layer Design

Gold tables represent **versioned, model-ready datasets** consumed by analysts.
We do **not** want these to be overwritten with bad data.

Gold is intentionally thin:
- It loads Silver2,
- Runs validation,
- Publishes data directly from silver2 to `final/` gold table.
- Updates the version registry.

---

# Versioning & the Registry

The project includes a lightweight versioning system stored in:

```
log/registry.json
```

Each gold publish increments an integer version counter.  

A registry entry looks like:

```json
{
  "gold_pitching": {
    "version": 3,
    "timestamp": "2025-01-12T14:32:00",
    "row_count": 157614,
    "columns": [...]
  }
}
```

### Why keep versioning?

- Ensures we don’t silently overwrite gold outputs  
- Enables schema drift detection  
- Provides metadata for auditing  
- Maintains explicit “publishing events” without storing full history  

This mirrors production MLOps / data publishing practices.If we had more time, we could make this much more robust!

---

# Validations

Before publishing gold, the pipeline checks:

- silver2 not empty  
- required columns present  
- basic numeric sanity checks  
- schema consistency with previous gold version  

Warnings are printed for schema drift. We only use best practices over here!

---

# Summary of Key Decisions & Fixes

### **Fixed**
- Duplicate-column bug caused by dropping columns in snake-case  
- Missing `at_bat_index` leading to Silver1 failures  
- Bronze schema inconsistencies handled via deterministic renaming  

### **Design Choices**
- Silver1 = cleaned, basic, pitch-level table  
- Silver2 = feature engineering for modeling  
- Gold = thin, versioned publishing layer  
- Registry = metadata-only versioning store

---

# Running the Silver & Gold Pipelines

### Silver:
```sh
python pipelines/silver.py
```

Outputs:
- `final/silver_core.parquet`
- `intermediate/silver_pitching.parquet`
- `intermediate/silver_batting.parquet`

### Gold:
```sh
python pipelines/gold_pitching.py
python pipelines/gold_batting.py
```

Outputs:
- `final/gold_pitching.parquet`
- `final/gold_batting.parquet`
- updated `log/registry.json`

**NOTE: The python packages needed for these runs to succeed are listed in requirements.txt **

---

P.S. - Thank you for letting me show you my stuff on this project. Go White Sox! -Rob