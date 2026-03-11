# Predictive Maintenance Data Pipeline

This repository contains a simple data engineering pipeline for processing industrial sensor data used in predictive maintenance.

The project defines a **data contract**, validates raw sensor data, cleans it, and generates features for further analysis or machine learning.

---

# Project Overview

The pipeline processes sensor time series data in three main steps:

```
export_raw.py
        ↓
raw.parquet
        ↓
validate_and_clean.py
        ↓
clean.parquet + quality_report.json
        ↓
build_features.py
        ↓
features.parquet
```

Each stage transforms the dataset and prepares it for the next processing step.

---

# Repository Structure

```
PredictiveMaintanence
│
├── data_contract_v1.md
│
├── data
│   ├── example_raw.parquet
│   ├── raw.parquet
│   ├── clean.parquet
│   └── features.parquet
│
├── reports
│   └── quality_report.json
│
├── src
│   ├── export_raw.py
│   ├── validate_and_clean.py
│   └── build_features.py
│
├── tests
│   └── test_pipeline.py
│
├── requirements.txt
└── README.md
```

---

# Data Contract

The dataset schema and validation rules are defined in:

```
data_contract_v1.md
```

The contract specifies:

- mandatory columns
- naming conventions
- sampling rates
- valid measurement ranges
- data quality rules
- cleaning rules

Any dataset violating this contract must be rejected.

---

# Data Pipeline

## 1 Export Raw Data

Script:

```
src/export_raw.py
```

Purpose:

- create or load raw sensor data
- store dataset as

```
data/raw.parquet
```

Raw data may contain:

- missing values
- duplicates
- outliers
- sensor anomalies

---

## 2 Validate and Clean Data

Script:

```
src/validate_and_clean.py
```

Input:

```
data/raw.parquet
```

Output:

```
data/clean.parquet
reports/quality_report.json
```

Cleaning steps:

- duplicate removal
- missing value handling
- outlier detection
- sensor stuck detection
- jump detection

The script adds a column:

```
quality_flag
```

Meaning:

| flag | meaning |
|-----|--------|
| 0 | valid |
| 1 | suspicious |
| 2 | invalid |

---

## 3 Feature Generation

Script:

```
src/build_features.py
```

Input:

```
data/clean.parquet
```

Output:

```
data/features.parquet
```

Generated features include:

- mean
- standard deviation
- minimum
- maximum
- RMS
- percentiles
- signal slope

These features are used for predictive maintenance analysis.

---

# Requirements

Install dependencies:

```
pip install -r requirements.txt
```

Required libraries:

- pandas
- numpy
- pyarrow
- scipy

---

# Running the Pipeline

Execute scripts in the following order:

```
python src/export_raw.py
python src/validate_and_clean.py
python src/build_features.py
```

---

# Testing

Basic pipeline tests are located in:

```
tests/test_pipeline.py
```

---

# Purpose of the Project

This repository demonstrates a simplified industrial data pipeline for predictive maintenance applications.

It focuses on:

- data contracts
- data validation
- cleaning sensor time series
- feature extraction
