# Data Contract v1
Predictive Maintenance Sensor Data Standard

Version: 1.0  
Owner: Data Engineering Team  

This document defines the **mandatory schema, naming conventions, sampling rules and data quality constraints** for sensor datasets used in the predictive maintenance pipeline.

The contract guarantees compatibility with downstream processing:

```
export_raw.py → raw.parquet
validate_and_clean.py → clean.parquet
build_features.py → features.parquet
```

Any dataset violating this contract must be rejected.

---

# 1 Dataset Overview

Sensor datasets contain time series measurements collected from industrial assets.

Each row represents **one measurement from one sensor at a specific timestamp**.

Example:

| ts | sensor_id | value |
|----|-----------|------|
| 2025-01-01T10:00:00Z | pump01_temp_motor | 63.2 |

---

# 2 Mandatory Columns

Datasets must contain **exactly the following columns**.

No additional columns are allowed.

| column | type | description |
|------|------|-------------|
| ts | timestamp | measurement timestamp (UTC) |
| sensor_id | string | unique sensor identifier |
| value | float | measured value |

clean.parquet contains an additional column:

| quality_flag | int | data quality indicator (clean dataset only) |

Example row (raw.parquet):

| ts | sensor_id | value |
|----|-----------|------|
| 2025-01-01T10:00:00Z | pump01_temp_motor | 63.2 |

Example row (clean.parquet):

| ts | sensor_id | value | quality_flag |
|----|-----------|------|--------------|
| 2025-01-01T10:00:00Z | pump01_temp_motor | 63.2 | 0 |

---

# 3 Data Types

| column | datatype | constraints |
|------|-----------|-------------|
| ts | timestamp (ISO 8601) | must be UTC |
| sensor_id | string | must follow naming convention |
| value | float64 | numeric sensor measurement |

Rules:

- timestamps must be parseable
- timestamps must be sortable
- sensor_id must follow naming convention
- value must be numeric

Example timestamp:

```
2025-01-01T10:00:00Z
```

---

# 4 Structural Rules

## 4.1 Unique Measurement Constraint

The combination

```
(ts, sensor_id)
```

must be unique.

Duplicates may appear in **raw.parquet**, but must be resolved during cleaning.

---

## 4.2 No Additional Columns

Datasets must contain **only the mandatory columns**.

If additional columns exist:

```
dataset must be rejected
```

---

## 4.3 Missing Values

The column `value` must exist.

Raw datasets may contain missing values, but they will be handled during cleaning.

---

## 4.4 Timestamp Requirements

- timestamps must be UTC
- timestamps must follow ISO-8601
- timestamps must be sortable

Example:

```
2025-01-01T10:00:00Z
```

---

# 5 Sensor Naming Convention

All sensor identifiers must follow the pattern:

```
<asset>_<sensorType>_<position>
```

Examples:

```
pump01_temp_motor
pump01_vib_x
pump01_vib_y
pump01_current_l1
pump01_pressure_inlet
```

Naming rules:

| rule | description |
|----|-------------|
| lowercase only | no uppercase characters |
| underscore separator | `_` used between elements |
| no spaces | spaces forbidden |
| no special characters | except `_` |
| consistent asset prefix | same asset naming |

Allowed characters:

```
a-z
0-9
_
```

Recommended validation regex:

```
^[a-z0-9]+_[a-z0-9]+_[a-z0-9]+$
```

---

# 6 Asset Identification

The asset identifier is extracted from `sensor_id`.

Example:

```
pump01_temp_motor
```

Asset ID:

```
pump01
```

This value will be used later in:

```
features.parquet
```

---

# 7 Sampling Definition

Each sensor has an expected sampling rate and valid measurement range.

| sensor_id | sampling_rate | expected_min | expected_max |
|-----------|---------------|--------------|--------------|
| pump01_temp_motor | 1 | -20 | 150 |
| pump01_vib_x | 5000 | -50 | 50 |
| pump01_vib_y | 5000 | -50 | 50 |
| pump01_current_l1 | 100 | 0 | 100 |
| pump01_pressure_inlet | 10 | 0 | 25 |

Definitions:

**sampling_rate**

expected measurement frequency in **measurements per second (Hz)**.

Example:

- sampling_rate = 1 → one measurement per second
- sampling_rate = 5000 → 5000 measurements per second

**expected_min**

minimum plausible measurement.

**expected_max**

maximum plausible measurement.

Values outside this range are considered **outliers**.

---

# 8 Unit Standardization

All sensors must use standardized units.

| measurement | unit |
|-------------|------|
| temperature | C |
| vibration | mm_s |
| current | A |
| pressure | bar |
| time | s or ms |

Forbidden inconsistencies:

```
C vs °C vs degC
```

Each sensor type must use **one canonical unit**.

---

# 9 Data Quality Issues

Raw sensor datasets may contain quality issues.

These are handled during the cleaning stage.

Defined issues:

| issue | description |
|------|-------------|
| missing values | NULL or NaN |
| duplicates | identical (ts, sensor_id) |
| outliers | outside expected range |
| sensor stuck | constant value ≥ 30 seconds |
| signal jumps | sudden abnormal changes |

Cleaning is implemented in:

```
validate_and_clean.py
```

---

# 10 Duplicate Handling

Duplicates are defined as:

```
same (ts, sensor_id)
```

Cleaning rule:

```
average value
keep one row
```

---

# 11 Missing Sensor Data

Definition:

```
value = NULL or NaN
```

Cleaning rules:

| gap duration | action |
|--------------|--------|
| ≤ 5 seconds | linear interpolation |
| > 5 seconds | value = null |

Quality flag assignment:

| condition | flag |
|-----------|------|
| interpolated | 1 |
| missing | 2 |

---

# 12 Outlier Handling

Definition:

```
value outside expected_min / expected_max
```

Cleaning rule:

```
value = null
quality_flag = 2
```

---

# 13 Sensor Stuck Detection

Definition:

```
identical value for ≥ 30 seconds
```

Action:

```
quality_flag = 2
```

---

# 14 Jump Detection

Definition:

```
|value(t) − value(t−1)| > 5 × rolling_std(60s)
```

Action:

```
quality_flag = 1
value remains unchanged
```

---

# 15 Quality Flag Definition

| flag | meaning |
|-----|---------|
| 0 | valid measurement |
| 1 | suspicious but usable |
| 2 | invalid measurement |

The column `quality_flag` is added during the cleaning stage.

---

# 16 Raw Dataset Definition

`raw.parquet` must satisfy:

- correct schema
- correct datatypes
- valid sensor naming

However it may contain:

- duplicates
- missing values
- outliers
- stuck sensors
- signal jumps

---

# 17 Clean Dataset Definition

`clean.parquet` must contain:

- resolved duplicates
- interpolated missing values
- flagged outliers
- detected sensor anomalies
- column `quality_flag`

The dataset must be **reproducible from raw.parquet**.

---

# 18 Versioning

Data contracts must follow versioning.

Example:

```
data_contract_v1.md
data_contract_v2.md
```

Changes require:

- pull request
- documentation update
- compatibility check

---

# 19 Compliance

Any dataset violating this contract must be rejected during ingestion.

Validation will be implemented in:

```
validate_and_clean.py
```

---

# End of Data Contract
