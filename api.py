from fastapi import FastAPI
from datetime import datetime, timezone
import random
import threading
import time
from collections import deque

app = FastAPI()

# -----------------------------
# WITSML-style metadata
# -----------------------------
WELL_UID = "WELL-001"
WELLBORE_UID = "WELLBORE-001"
LOG_UID = "REALTIME-DRILLING-LOG"
RIG_ID = "RIG-001"

# -----------------------------
# In-memory WITSML-like log buffer
# Keeps only latest 1000 rows to avoid unlimited growth
# -----------------------------
log_rows = deque(maxlen=50)
sequence_number = 0
data_lock = threading.Lock()

# -----------------------------
# Latest drilling snapshot
# -----------------------------
drilling_data = {
    "well_uid": WELL_UID,
    "wellbore_uid": WELLBORE_UID,
    "log_uid": LOG_UID,
    "rig_id": RIG_ID,
    "timestamp": "",
    "measured_depth_m": 1500.0,
    "rop_m_per_hr": 15.0,
    "wob_klbf": 30.0,
    "rpm": 120,
    "torque_ftlb": 25000,
    "standpipe_pressure_psi": 2800,
    "mud_flow_lpm": 3200,
    "hookload_klbf": 200
}


# -----------------------------
# Data generator
# -----------------------------
def generate_data():
    global sequence_number

    while True:
        with data_lock:
            sequence_number += 1

            # Increase depth continuously
            drilling_data["measured_depth_m"] += random.uniform(0.1, 0.5)

# Mostly good data, sometimes bad for testing the bad topic
            is_bad = random.random() < 0.15  # 15% bad, 85% clean

            if is_bad:
                drilling_data["rop_m_per_hr"] = round(random.uniform(-5, 25), 2)
                drilling_data["wob_klbf"] = round(random.uniform(-10, 40), 2)
                drilling_data["rpm"] = random.choice([
                    random.randint(301, 500),
                    random.randint(-50, -1)
                ])
                drilling_data["torque_ftlb"] = random.randint(-1000, 30000)
                drilling_data["standpipe_pressure_psi"] = random.randint(-500, 3500)
                drilling_data["mud_flow_lpm"] = random.randint(-100, 3500)
                drilling_data["hookload_klbf"] = random.randint(-50, 240)

            else:
                drilling_data["rop_m_per_hr"] = round(random.uniform(10, 25), 2)
                drilling_data["wob_klbf"] = round(random.uniform(20, 40), 2)
                drilling_data["rpm"] = random.randint(100, 140)
                drilling_data["torque_ftlb"] = random.randint(20000, 30000)
                drilling_data["standpipe_pressure_psi"] = random.randint(2500, 3500)
                drilling_data["mud_flow_lpm"] = random.randint(3000, 3500)
                drilling_data["hookload_klbf"] = random.randint(180, 240)

            # Timestamp in UTC
            timestamp = datetime.now(timezone.utc).isoformat()
            drilling_data["timestamp"] = timestamp

            # WITSML-like log row
            log_row = {
                "sequence_number": sequence_number,
                "well_uid": WELL_UID,
                "wellbore_uid": WELLBORE_UID,
                "log_uid": LOG_UID,
                "rig_id": RIG_ID,
                "index_type": "date time",
                "index_mnemonic": "TIME",
                "mnemonic_list": [
                    "TIME",
                    "MD",
                    "ROP",
                    "WOB",
                    "RPM",
                    "TORQUE",
                    "SPP",
                    "FLOW",
                    "HKLD"
                ],
                "unit_list": [
                    "iso8601",
                    "m",
                    "m/h",
                    "klbf",
                    "rpm",
                    "ft-lbf",
                    "psi",
                    "L/min",
                    "klbf"
                ],
                "data": [
                    timestamp,
                    round(drilling_data["measured_depth_m"], 2),
                    drilling_data["rop_m_per_hr"],
                    drilling_data["wob_klbf"],
                    drilling_data["rpm"],
                    drilling_data["torque_ftlb"],
                    drilling_data["standpipe_pressure_psi"],
                    drilling_data["mud_flow_lpm"],
                    drilling_data["hookload_klbf"]
                ]
            }

            log_rows.append(log_row)

        # Generate every second
        time.sleep(1)


# Run generator in background
thread = threading.Thread(target=generate_data, daemon=True)
thread.start()


# -----------------------------
# API Endpoints
# -----------------------------
@app.get("/")
def home():
    return {"message": "Simulated WITSML-like Server Running"}


@app.get("/well/current")
def get_current_data():
    with data_lock:
        return drilling_data.copy()


@app.get("/witsml/logs")
def get_witsml_logs(after_sequence: int = 0):
    with data_lock:
        new_rows = [
            row for row in log_rows
            if row["sequence_number"] > after_sequence
        ]

    return {
        "well_uid": WELL_UID,
        "wellbore_uid": WELLBORE_UID,
        "log_uid": LOG_UID,
        "count": len(new_rows),
        "latest_sequence": sequence_number,
        "rows": new_rows
    }