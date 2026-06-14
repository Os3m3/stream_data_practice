from confluent_kafka import Consumer
from logger import log
import psycopg
import os
from dotenv import load_dotenv


consumer = Consumer({
    "bootstrap.servers" : "localhost:9092",
    "group.id" : "database_group",
    "auto.offset.reset" : "earliest"
})

consumer.subscribe(['clean-drillinf-data'])

db_conn = psycopg.connect(
    host="localhost",
    port=5432,
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

cursor = db_conn.cursor()

cursor.execute=(""" 
CREATE TABLE IF NOT EXIST clean_drilling_data (
   sequence_number BIGINT PRIMARY KEY,
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
    hookload_klbf DOUBLE PRECISION               
);
""")

db_conn.commit()

cursor.execute=("""
SELECT create_hypertable(
  'clean_drilling_data',
  'time'
);                
""")

db_conn.commit()

log.info("Database consumer started polling")
log.info("clean_drilling_data table is ready")

