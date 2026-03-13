# Predictive Maintenance Pipeline

## Überblick

Dieses Repository enthält eine modulare Datenpipeline für ein Predictive-Maintenance-System. Ziel ist es, Sensordaten strukturiert aufzubereiten, zu validieren, zu bereinigen und daraus Features für spätere Machine-Learning-Modelle zu generieren.

Die Pipeline verfolgt folgende Designprinzipien:

- reproduzierbare Datenverarbeitung
- klare Trennung der Verarbeitungsschritte
- strikte Schema- und Datenvalidierung
- konfigurierbare Feature-Generierung
- transparente Data-Quality-Berichte
- modulare Erweiterbarkeit für spätere Modelle

Die Pipeline verarbeitet Rohdaten Schritt für Schritt bis zu einem Feature-Datensatz, der später für Modelltraining oder Inferenz genutzt werden kann.

---

# Pipeline-Architektur

Die Daten durchlaufen mehrere klar getrennte Verarbeitungsschritte.

Raw Data  
→ Ingestion  
→ Schema Validation  
→ Value Validation  
→ Data Cleaning  
→ Feature Engineering  
→ Feature Dataset

Jeder Schritt erzeugt ein eigenes Artefakt im `data/` Verzeichnis, sodass Zwischenergebnisse jederzeit überprüft werden können.

---

# Projektstruktur

PredictiveMaintanence/

configs/  
 data_contract.yaml  
 feature_config.yaml  
 validation_rules.yaml  

data/  
 raw/  
  raw.parquet  

 validated/  
  validated.parquet  

 clean/  
  clean.parquet  

 features/  
  features.parquet  

reports/  
 data_quality/  
  quality_report.json  

src/  
 ingestion/  
  export_raw.py  

 validation/  
  validate_schema.py  
  validate_values.py  
  quality_flags.py  
  write_quality_report.py  

 cleaning/  
  clean_data.py  

 features/  
  build_features.py  

 utils/  
  paths.py  

 run_pipeline.py  

tests/  
 test_pipeline.py  

requirements.txt  
README.md  

---

# Pipeline-Ausführung

Die gesamte Pipeline wird zentral über `run_pipeline.py` gestartet.

Ausführung aus dem Repository-Root:

python -m src.run_pipeline

Die Pipeline führt dabei nacheinander folgende Schritte aus:

1. Rohdaten exportieren oder einlesen
2. Schema validieren
3. Werte validieren
4. Quality Flags erzeugen
5. Data Quality Report schreiben
6. Daten bereinigen
7. Features generieren

---

# Datenartefakte

Während der Pipeline entstehen mehrere definierte Datensätze.

Rohdaten  
data/raw/raw.parquet

Validierte Daten  
data/validated/validated.parquet

Bereinigte Daten  
data/clean/clean.parquet

Feature-Datensatz  
data/features/features.parquet

Data Quality Bericht  
reports/data_quality/quality_report.json

---

# Konfigurationsdateien

Die Pipeline ist stark konfigurationsbasiert aufgebaut.

configs/data_contract.yaml  
Definiert das erwartete Datenschema der Rohdaten.

configs/validation_rules.yaml  
Definiert Validierungsregeln für Sensordaten.

configs/feature_config.yaml  
Definiert Feature-Fenster und Featuretypen für das Feature Engineering.

---

# Zentrale Pfadverwaltung

Alle Pfade werden zentral über

src/utils/paths.py

definiert.

Dadurch können alle Skripte auf konsistente Projektpfade zugreifen, ohne harte Dateipfade zu verwenden.

Beispielsweise:

DATA_DIR  
RAW_DIR  
VALIDATED_DIR  
CLEAN_DIR  
FEATURE_DIR  
REPORT_DIR  
CONFIG_DIR  

Neue Skripte sollten immer diese Pfaddefinitionen verwenden.

---

# Data Quality Konzept

Die Pipeline implementiert mehrere Qualitätsmechanismen.

Schema Validation  
Überprüft Struktur und Datentypen der Rohdaten.

Value Validation  
Überprüft Sensordaten auf zulässige Wertebereiche.

Quality Flags  
Kennzeichnen potenziell fehlerhafte Messungen.

Quality Report  
Erzeugt eine zusammenfassende Qualitätsanalyse.

Der Quality Report wird als JSON gespeichert:

reports/data_quality/quality_report.json

---

# Tests

Das Repository enthält erste Pipeline-Tests.

tests/test_pipeline.py

Die Tests überprüfen aktuell:

- Datenstruktur
- Pipeline-Ausführung
- grundlegende Feature-Generierung

---

# Erweiterungsplanung

Die aktuelle Pipeline deckt folgende Bereiche ab:

Ingestion  
Validation  
Cleaning  
Feature Engineering  
Quality Reporting

Geplante Erweiterungen:

Model Training  
Model Evaluation  
Train/Test Splitting  
Model Registry  
Prediction Pipeline  
Monitoring

Der Ordner `src/models` ist bereits für diese Erweiterungen vorgesehen.

---

# Ziel des Projekts

Die Pipeline bildet die Grundlage für ein skalierbares Predictive-Maintenance-System.

Langfristig sollen darauf aufbauend folgende Komponenten integriert werden:

- ML-Modelle zur Ausfallvorhersage
- Remaining Useful Life Modelle
- Online-Inferenz für Echtzeitdaten
- Monitoring von Daten- und Modellqualität