# Data Contract v1
Standard für Sensordaten in Predictive-Maintenance-Anwendungen

Version: 1.0  
Verantwortlich: Data Engineering Team  

Dieses Dokument definiert das **verpflichtende Schema, Naming-Konventionen, Sampling-Regeln sowie Datenqualitätsanforderungen** für Sensordatensätze, die in der Predictive-Maintenance-Pipeline verwendet werden.

Der Vertrag garantiert die Kompatibilität mit der nachgelagerten Datenverarbeitung:

```
export_raw.py → raw.parquet
validate_and_clean.py → clean.parquet
build_features.py → features.parquet
```

Datensätze, die diesen Vertrag verletzen, müssen zurückgewiesen werden.

---

# 1 Überblick über den Datensatz

Sensordatensätze enthalten Zeitreihenmessungen, die von industriellen Anlagen erfasst werden.

Jede Zeile repräsentiert **eine Messung eines Sensors zu einem bestimmten Zeitpunkt**.

Beispiel:

| ts | sensor_id | value |
|----|-----------|------|
| 2025-01-01T10:00:00Z | pump01_temp_motor | 63.2 |

---

# 2 Verpflichtende Spalten

Datensätze müssen **genau die folgenden Spalten enthalten**.

Weitere Spalten sind nicht erlaubt.

| column | type | description |
|------|------|-------------|
| ts | timestamp | Zeitstempel der Messung (UTC) |
| sensor_id | string | eindeutige Sensor-ID |
| value | float | gemessener Sensorwert |

`clean.parquet` enthält zusätzlich eine weitere Spalte:

| quality_flag | int | Indikator für die Datenqualität (nur im bereinigten Datensatz) |

Beispielzeile (`raw.parquet`):

| ts | sensor_id | value |
|----|-----------|------|
| 2025-01-01T10:00:00Z | pump01_temp_motor | 63.2 |

Beispielzeile (`clean.parquet`):

| ts | sensor_id | value | quality_flag |
|----|-----------|------|--------------|
| 2025-01-01T10:00:00Z | pump01_temp_motor | 63.2 | 0 |

---

# 3 Datentypen

| column | datatype | constraints |
|------|-----------|-------------|
| ts | timestamp (ISO 8601) | muss UTC sein |
| sensor_id | string | muss der Naming-Konvention folgen |
| value | float64 | numerischer Sensorwert |

Bereinigter Datensatz (`clean.parquet`) enthält zusätzlich:

| column | datatype | description |
|------|-----------|-------------|
| quality_flag | int | Kennzeichnung der Datenqualität |

Regeln:

- Zeitstempel müssen parsebar sein
- Zeitstempel müssen sortierbar sein
- sensor_id muss der Naming-Konvention entsprechen
- value muss numerisch sein

Beispiel-Zeitstempel:

```
2025-01-01T10:00:00Z
```

---

# 4 Strukturelle Regeln

## 4.1 Eindeutige Messung

Die Kombination

```
(ts, sensor_id)
```

muss eindeutig sein.

Duplikate können in **raw.parquet** vorkommen, müssen jedoch während der Bereinigung entfernt werden.

---

## 4.2 Keine zusätzlichen Spalten

Datensätze dürfen **nur die verpflichtenden Spalten** enthalten.

Falls zusätzliche Spalten vorhanden sind:

```
Datensatz muss abgelehnt werden
```

---

## 4.3 Fehlende Werte

Die Spalte `value` muss vorhanden sein.

Rohdatensätze können fehlende Werte enthalten, diese werden jedoch während der Datenbereinigung behandelt.

---

## 4.4 Anforderungen an Zeitstempel

- Zeitstempel müssen UTC sein
- Zeitstempel müssen dem ISO-8601-Format folgen
- Zeitstempel müssen sortierbar sein

Beispiel:

```
2025-01-01T10:00:00Z
```

---

# 5 Naming-Konvention für Sensoren

Alle Sensor-IDs müssen dem Muster folgen:

```
<asset>_<sensorType>_<position>
```

Beispiele:

```
pump01_temp_motor
pump01_vib_x
pump01_vib_y
pump01_current_l1
pump01_pressure_inlet
```

Naming-Regeln:

| rule | description |
|----|-------------|
| nur Kleinbuchstaben | keine Großbuchstaben |
| Unterstrich als Trenner | `_` wird verwendet |
| keine Leerzeichen | Leerzeichen sind nicht erlaubt |
| keine Sonderzeichen | außer `_` |
| konsistenter Asset-Präfix | gleiche Maschinenbezeichnung |

Erlaubte Zeichen:

```
a-z
0-9
_
```

Empfohlene Validierungs-Regex:

```
^[a-z0-9]+_[a-z0-9]+_[a-z0-9]+$
```

---

# 6 Identifikation der Anlage (Asset)

Die Anlagen-ID wird aus `sensor_id` extrahiert.

Beispiel:

```
pump01_temp_motor
```

Asset-ID:

```
pump01
```

Dieser Wert wird später verwendet in:

```
features.parquet
```

---

# 7 Sampling-Definition

Jeder Sensor besitzt eine erwartete Samplingrate sowie gültige Messbereiche.

| sensor_id | sampling_rate | expected_min | expected_max |
|-----------|---------------|--------------|--------------|
| pump01_temp_motor | 1 | -20 | 150 |
| pump01_vib_x | 5000 | -50 | 50 |
| pump01_vib_y | 5000 | -50 | 50 |
| pump01_current_l1 | 100 | 0 | 100 |
| pump01_pressure_inlet | 10 | 0 | 25 |

Definitionen:

**sampling_rate**

Erwartete Messfrequenz in **Messungen pro Sekunde (Hz)**.

Beispiel:

- sampling_rate = 1 → eine Messung pro Sekunde  
- sampling_rate = 5000 → 5000 Messungen pro Sekunde  

**expected_min**

Minimal plausibler Messwert.

**expected_max**

Maximal plausibler Messwert.

Werte außerhalb dieses Bereichs gelten als **Ausreißer**.

---

# 8 Standardisierung von Einheiten

Alle Sensoren müssen standardisierte Einheiten verwenden.

| measurement | unit |
|-------------|------|
| temperature | C |
| vibration | mm_s |
| current | A |
| pressure | bar |
| time | s oder ms |

Unzulässige Inkonsistenzen:

```
C vs °C vs degC
```

Jeder Sensortyp darf **nur eine einheitliche Standard-Einheit** verwenden.

---

# 9 Probleme mit der Datenqualität

Rohdatensätze können Qualitätsprobleme enthalten.

Diese werden während der Datenbereinigung behandelt.

Definierte Probleme:

| issue | description |
|------|-------------|
| missing values | NULL oder NaN |
| duplicates | identische Kombination (ts, sensor_id) |
| outliers | außerhalb des erwarteten Wertebereichs |
| sensor stuck | konstanter Wert ≥ 30 Sekunden |
| signal jumps | plötzliche Signaländerungen |

Die Bereinigung wird implementiert in:

```
validate_and_clean.py
```

---

# 10 Behandlung von Duplikaten

Duplikate sind definiert als:

```
gleiche Kombination (ts, sensor_id)
```

Bereinigungsregel:

```
Mittelwert bilden
eine Zeile behalten
```

---

# 11 Fehlende Sensordaten

Definition:

```
value = NULL oder NaN
```

Bereinigungsregeln:

| Lückendauer | Aktion |
|--------------|--------|
| ≤ 5 Sekunden | lineare Interpolation |
| > 5 Sekunden | value = null |

Zuweisung von quality_flag:

| Bedingung | flag |
|-----------|------|
| interpoliert | 1 |
| fehlend | 2 |

---

# 12 Behandlung von Ausreißern

Definition:

```
value außerhalb expected_min / expected_max
```

Bereinigungsregel:

```
value = null
quality_flag = 2
```

---

# 13 Erkennung festhängender Sensoren

Definition:

```
identischer Wert über ≥ 30 Sekunden
```

Aktion:

```
quality_flag = 2
```

---

# 14 Erkennung von Signalsprüngen

Definition:

```
|value(t) − value(t−1)| > 5 × rolling_std(60s)
```

Aktion:

```
quality_flag = 1
value bleibt unverändert
```

---

# 15 Definition der Quality Flags

| flag | Bedeutung |
|-----|-----------|
| 0 | gültige Messung |
| 1 | verdächtige, aber nutzbare Messung |
| 2 | ungültige Messung |

Die Spalte `quality_flag` wird während der Bereinigung hinzugefügt.

---

# 16 Definition des Rohdatensatzes

`raw.parquet` muss erfüllen:

- korrektes Schema
- korrekte Datentypen
- gültige Sensorbenennung

Darf jedoch enthalten:

- Duplikate
- fehlende Werte
- Ausreißer
- festhängende Sensoren
- Signalsprünge

---

# 17 Definition des bereinigten Datensatzes

`clean.parquet` muss enthalten:

- entfernte Duplikate
- interpolierte fehlende Werte
- markierte Ausreißer
- erkannte Sensoranomalien
- Spalte `quality_flag`

Der Datensatz muss **reproduzierbar aus raw.parquet erzeugbar** sein.

---

# 18 Versionierung

Data Contracts müssen versioniert werden.

Beispiel:

```
data_contract_v1.md
data_contract_v2.md
```

Änderungen erfordern:

- Pull Request
- Aktualisierung der Dokumentation
- Kompatibilitätsprüfung der Pipeline

---

# 19 Einhaltung des Vertrags

Datensätze, die diesen Vertrag verletzen, müssen während der Datenaufnahme abgelehnt werden.

Die Validierung wird implementiert in:

```
validate_and_clean.py
```

---

# Ende des Data Contracts
