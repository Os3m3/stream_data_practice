from confluent_kafka import Consumer
from logger import log
import psycopg
import os
from dotenv import load_dotenv
import json

load_dotenv()  

consumer = Consumer({
    "bootstrap.servers" : "localhost:9092",
    "group.id" : "database_group",
    "auto.offset.reset" : "earliest"
})

consumer.subscribe(['clean-drilling-data'])

db_conn = psycopg.connect(
    host="localhost",
    port=5432,
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

cursor = db_conn.cursor()

cursor.execute("DROP TABLE IF EXISTS clean_drilling_data CASCADE;")
db_conn.commit()

cursor.execute(""" 
CREATE TABLE IF NOT EXISTS clean_drilling_data (
    sequence_number BIGINT,
    well_uid TEXT,
    wellbore_uid TEXT,
    log_uid TEXT,
    rig_id TEXT,
    time TIMESTAMPTZ NOT NULL,
    measured_depth_m DOUBLE PRECISION,
    rop_m_per_hr DOUBLE PRECISION,
    wob_klbf DOUBLE PRECISION,
    rpm INTEGER,
    torque_ftlb DOUBLE PRECISION,
    standpipe_pressure_psi DOUBLE PRECISION,
    mud_flow_lpm DOUBLE PRECISION,
    hookload_klbf DOUBLE PRECISION,
    PRIMARY KEY (sequence_number, time)               
);
""")

db_conn.commit()

cursor.execute("""
SELECT create_hypertable(
  'clean_drilling_data',
  'time'
);                
""")

db_conn.commit()

log.info("Database consumer started polling")
log.info("clean_drilling_data table is ready")

while True:
    try:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Consumer error:", msg.error())
            continue

        msg_text = msg.value().decode("utf-8")
        clean_data = json.loads(msg_text)

        values = clean_data["data"]

        timestamp = values[0]
        md = values[1]
        rop = values[2]
        wob = values[3]
        rpm = values[4]
        torque = values[5]
        spp = values[6]
        flow = values[7]
        hkld = values[8]

        cursor.execute(
            """
            INSERT INTO clean_drilling_data (
                sequence_number,
                well_uid,
                wellbore_uid,
                log_uid,
                rig_id,
                time,
                measured_depth_m,
                rop_m_per_hr,
                wob_klbf,
                rpm,
                torque_ftlb,
                standpipe_pressure_psi,
                mud_flow_lpm,
                hookload_klbf
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sequence_number, time) DO NOTHING;
            """,
            (
                clean_data["sequence_number"],
                clean_data["well_uid"],
                clean_data["wellbore_uid"],
                clean_data["log_uid"],
                clean_data["rig_id"],
                timestamp,
                md,
                rop,
                wob,
                rpm,
                torque,
                spp,
                flow,
                hkld
            )
        )

        db_conn.commit()

        log.info(f"Inserted clean record: {clean_data['sequence_number']}")
        print("Inserted clean record:", clean_data["sequence_number"])

    except Exception as e:
        print("Error:", e)
        db_conn.rollback()