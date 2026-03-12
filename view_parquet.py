import pandas as pd
import matplotlib.pyplot as plt

# Dateien laden
df_raw = pd.read_parquet("data/raw.parquet")
df_clean = pd.read_parquet("data/clean.parquet")
df_features = pd.read_parquet("data/features.parquet")

# Zeitstempel in datetime umwandeln
df_raw["ts"] = pd.to_datetime(df_raw["ts"])
df_clean["ts"] = pd.to_datetime(df_clean["ts"])

# Beispiel 1: Rohdaten eines Sensors plotten
sensor_name = "pump01_temp_motor"
df_sensor_raw = df_raw[df_raw["sensor_id"] == sensor_name].sort_values("ts")

plt.figure(figsize=(10, 4))
plt.plot(df_sensor_raw["ts"], df_sensor_raw["value"])
plt.title(f"Raw Data - {sensor_name}")
plt.xlabel("Timestamp")
plt.ylabel("Value")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Beispiel 2: Bereinigte Daten desselben Sensors plotten
df_sensor_clean = df_clean[df_clean["sensor_id"] == sensor_name].sort_values("ts")

plt.figure(figsize=(10, 4))
plt.plot(df_sensor_clean["ts"], df_sensor_clean["value"])
plt.title(f"Clean Data - {sensor_name}")
plt.xlabel("Timestamp")
plt.ylabel("Value")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Beispiel 3: Features als Balkendiagramm
df_features_plot = df_features.set_index("sensor_id")

plt.figure(figsize=(10, 4))
df_features_plot["mean"].plot(kind="bar")
plt.title("Mean Feature per Sensor")
plt.xlabel("Sensor")
plt.ylabel("Mean")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()