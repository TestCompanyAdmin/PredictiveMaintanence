# Predictive Maintenance Pipeline -- Gesamtübersicht

Diese Dokumentation beschreibt den aktuellen Stand der entwickelten
Predictive-Maintenance-Datenpipeline vollständig und strukturiert.

Die Pipeline stellt eine technische Grundlage für folgende Aufgaben dar:

-   Rohdatenerzeugung / Ingestion
-   formale Datenvalidierung
-   fachliche Werteprüfung
-   Qualitätsklassifikation
-   Datenbereinigung
-   Feature Engineering
-   Qualitätsreporting
-   Pipeline-Orchestrierung

------------------------------------------------------------------------

# 1. Zielbild der Pipeline

Die Pipeline besteht aus mehreren Verarbeitungsschritten.

Rohdaten erzeugen\
↓\
Schema validieren\
↓\
Werte fachlich prüfen\
↓\
Quality Flags setzen\
↓\
Quality Report erzeugen\
↓\
Daten bereinigen\
↓\
Features erzeugen

Dabei entstehen definierte Datenzustände:

raw → validated → clean → features

------------------------------------------------------------------------

# 2. Projektstruktur

PredictiveMaintenance/

README.md\
requirements.txt\
configs/\
data/\
reports/\
src/\
tests/\
docs/\
archiv/

------------------------------------------------------------------------

# 3. Konfiguration

## data_contract.yaml

Definiert den formalen Datenvertrag:

-   Pflichtspalten
-   Datentypen
-   erlaubte Units
-   Pflichtfelder ohne Nullwerte

Beispielspalten:

ts\
asset_id\
sensor_id\
value\
unit

------------------------------------------------------------------------

## validation_rules.yaml

Definiert fachliche Regeln für Sensoren:

-   bekannte Assets
-   erlaubte Sensoren
-   erwartete Units
-   Wertebereiche
-   Samplingfrequenzen

Beispiel:

temp_motor → °C\
pressure_inlet → bar\
rpm_motor → rpm\
vibration_x → mm/s

------------------------------------------------------------------------

## feature_config.yaml

Konfiguriert das Feature Engineering.

Beispiele:

window_size_seconds\
step_size_seconds

Features:

mean\
std\
min\
max\
rms

------------------------------------------------------------------------

# 4. Datenverarbeitung

## Schritt 1 -- export_raw.py

Erzeugt simulierte Sensordaten.

Output:

data/raw/raw.parquet

Sensoren:

temp_motor\
pressure_inlet\
rpm_motor\
vibration_x\
vibration_y

Zusätzlich werden absichtlich Fehler erzeugt:

-   Missing Values
-   Outlier
-   Duplikate
-   Sensor Stuck

------------------------------------------------------------------------

## Schritt 2 -- validate_schema.py

Prüft den formalen Datenvertrag.

Input:

data/raw/raw.parquet

Prüft:

-   Pflichtspalten
-   Datentypen
-   Nullwerte
-   erlaubte Units

Output:

data/validated/validated.parquet

------------------------------------------------------------------------

## Schritt 3 -- validate_values.py

Prüft fachliche Regeln.

Kontrolliert:

-   unbekannte Sensoren
-   falsche Units
-   Werte außerhalb des Bereichs
-   Duplikate
-   Missing Values

Gibt aktuell nur Statistiken im Terminal aus.

------------------------------------------------------------------------

## Schritt 4 -- quality_flags.py

Markiert Qualitätsprobleme im Datensatz.

Neue Spalten:

quality_flag\
quality_reason\
is_usable

Typische Gründe:

unknown_sensor\
wrong_unit\
missing_value\
duplicate_row\
out_of_range

------------------------------------------------------------------------

## Schritt 5 -- write_quality_report.py

Erzeugt einen aggregierten Qualitätsreport.

Output:

reports/data_quality/quality_report.json

Beinhaltet:

row_count\
quality_flag_counts\
quality_reason_counts\
missing_by_sensor\
out_of_range_by_sensor\
duplicate_by_sensor\
wrong_unit_by_sensor

------------------------------------------------------------------------

## Schritt 6 -- clean_data.py

Erstellt den bereinigten Datensatz.

Filter:

is_usable == True

Output:

data/clean/clean.parquet

------------------------------------------------------------------------

## Schritt 7 -- build_features.py

Erzeugt Features aus Zeitfenstern.

Input:

data/clean/clean.parquet

Fensterlogik:

window_size_seconds\
step_size_seconds

Features:

mean\
std\
min\
max\
rms\
sample_count

Output:

data/features/features.parquet

------------------------------------------------------------------------

# 5. Pipeline Orchestrierung

src/run_pipeline.py steuert die Reihenfolge:

1 export_raw\
2 validate_schema\
3 validate_values\
4 quality_flags\
5 write_quality_report\
6 clean_data\
7 build_features

Start der Pipeline:

python -m src.run_pipeline

------------------------------------------------------------------------

# 6. Aktueller Reifegrad

Der aktuelle Stand entspricht einer **Engineering‑Pipeline für
Predictive Maintenance**.

Vorhanden:

-   modulare Architektur
-   konfigurationsbasierte Regeln
-   Quality Flagging
-   Data Quality Reporting
-   Feature Engineering

Noch offen:

-   Modelltraining
-   Modellinferenz
-   Edge Deployment
-   automatisierte Tests
-   Zeitreihenresampling
-   Multi‑Asset Simulation

------------------------------------------------------------------------

# 7. Datenfluss Gesamtübersicht

export_raw.py\
→ data/raw/raw.parquet

validate_schema.py\
→ data/validated/validated.parquet

validate_values.py\
→ Statistik

quality_flags.py\
→ data/validated/validated.parquet

write_quality_report.py\
→ reports/data_quality/quality_report.json

clean_data.py\
→ data/clean/clean.parquet

build_features.py\
→ data/features/features.parquet
