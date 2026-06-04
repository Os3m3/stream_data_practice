from fastapi import FastAPI
from datetime import datetime
import random
import threading
import time

app = FastAPI()

# -----------------------------
# Simulated drilling data
# -----------------------------
drilling_data = {
    "well_id": "PDO-WELL-001",
    "timestamp": "",
    "depth_m": 1500.0,
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
    while True:

        # Increase depth continuously
        drilling_data["depth_m"] += random.uniform(0.1, 0.5)

        # Random fluctuations
        drilling_data["rop_m_per_hr"] = round(random.uniform(10, 25), 2)
        drilling_data["wob_klbf"] = round(random.uniform(20, 40), 2)
        drilling_data["rpm"] = random.randint(100, 140)
        drilling_data["torque_ftlb"] = random.randint(20000, 30000)
        drilling_data["standpipe_pressure_psi"] = random.randint(2500, 3500)
        drilling_data["mud_flow_lpm"] = random.randint(3000, 3500)
        drilling_data["hookload_klbf"] = random.randint(180, 240)

        # Timestamp
        drilling_data["timestamp"] = datetime.utcnow().isoformat()

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
    return {"message": "Simulated WITSML Server Running"}


@app.get("/well/current")
def get_current_data():
    return drilling_data