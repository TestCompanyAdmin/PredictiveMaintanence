# Predictive-Maintenance Datenpipeline

Dieses Repository enthält eine einfache Data-Engineering-Pipeline zur Verarbeitung industrieller Sensordaten für Predictive-Maintenance-Anwendungen.

Das Projekt definiert einen **Data Contract**, validiert Rohdaten von Sensoren, bereinigt diese und erzeugt daraus Features für Analysen oder Machine-Learning-Anwendungen.

---

# Projektübersicht

Die Pipeline verarbeitet Sensordaten in drei Hauptschritten:

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

Jede Stufe transformiert den Datensatz und bereitet ihn für den nächsten Verarbeitungsschritt vor.

---

# Repository-Struktur

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

Das Schema der Sensordaten sowie die Validierungsregeln sind definiert in:

```
data_contract_v1.md
```

Der Data Contract legt fest:

- verpflichtende Spalten
- Naming-Konventionen für Sensoren
- Samplingraten
- zulässige Messbereiche
- Datenqualitätsregeln
- Regeln für die Datenbereinigung

Datensätze, die diesen Vertrag verletzen, dürfen nicht verarbeitet werden.

---

# Datenpipeline

## 1 Export der Rohdaten

Script:

```
src/export_raw.py
```

Aufgabe:

- Rohdaten von Sensoren erzeugen oder laden
- Speicherung als

```
data/raw.parquet
```

Die Rohdaten können enthalten:

- fehlende Werte
- doppelte Messungen
- Ausreißer
- Sensoranomalien

---

## 2 Validierung und Bereinigung der Daten

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

Bereinigungsschritte:

- Entfernen von Duplikaten
- Behandlung fehlender Werte
- Erkennung von Ausreißern
- Erkennung festhängender Sensoren
- Erkennung plötzlicher Signaländerungen

Das Skript fügt eine zusätzliche Spalte hinzu:

```
quality_flag
```

Bedeutung:

| Flag | Bedeutung |
|-----|-----------|
| 0 | gültige Messung |
| 1 | verdächtige Messung |
| 2 | ungültige Messung |

---

## 3 Feature-Generierung

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

Erzeugte Features können enthalten:

- Mittelwert
- Standardabweichung
- Minimum
- Maximum
- RMS
- Perzentile
- Signaltrend (Slope)

Diese Features können für Predictive-Maintenance-Analysen oder Machine Learning genutzt werden.

---

# Voraussetzungen

Abhängigkeiten installieren mit:

```
pip install -r requirements.txt
```

Benötigte Python-Bibliotheken:

- pandas
- numpy
- pyarrow
- scipy

---

# Pipeline ausführen

Die Skripte werden in folgender Reihenfolge ausgeführt:

```
python src/export_raw.py
python src/validate_and_clean.py
python src/build_features.py
```

---

# Tests

Einfache Pipeline-Tests befinden sich in:

```
tests/test_pipeline.py
```

---

# Zweck des Projekts

Dieses Repository demonstriert eine vereinfachte industrielle Datenpipeline für Predictive-Maintenance-Anwendungen.

Der Fokus liegt auf:

- Data Contracts
- Datenvalidierung
- Bereinigung von Sensordaten
- Feature-Generierung für Analysen
