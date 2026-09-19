# Block 2 - Propagator: consumes satellites.tle.raw, propagates with SGP4,
# publishes to satellites.position. Invalid messages go to satellites.dlq.
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import numpy as np
from confluent_kafka import Consumer, Producer
from sgp4.api import Satrec
from sgp4 import omm
from skyfield.api import EarthSatellite, load, wgs84

BROKER = os.getenv("KAFKA_BROKER", "kafka:19092")
TLE_TOPIC = os.getenv("TLE_TOPIC", "satellites.tle.raw")
POSITION_TOPIC = os.getenv("POSITION_TOPIC", "satellites.position")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "satellites.dlq")
GROUP_ID = os.getenv("GROUP_ID", "propagator")
TICK_S = float(os.getenv("TICK_MS", "1000")) / 1000.0

ts = load.timescale()
sats = {}  # norad_id -> (EarthSatellite, name)

consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
})
producer = Producer({"bootstrap.servers": BROKER})


def build_satellite(data):
    o = data.get("omm")
    if o:
        satrec = Satrec()
        omm.initialize(satrec, o)
        return EarthSatellite.from_satrec(satrec, ts)
    l1, l2 = data.get("tle_line1"), data.get("tle_line2")
    if l1 and l2:
        return EarthSatellite(l1, l2, data.get("name"), ts)
    raise ValueError("message has neither omm nor tle_line1/2")


def handle_tle(raw):
    try:
        data = json.loads(raw)
        sats[data["norad_id"]] = (build_satellite(data), data.get("name", data["norad_id"]))
    except Exception as err:
        producer.produce(DLQ_TOPIC, json.dumps({
            "failed_stage": "parse/satrec",
            "error": str(err),
            "original_payload": raw,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).encode())


def propagate_all():
    t = ts.now()
    iso = t.utc_iso()
    for norad_id, (sat, name) in sats.items():
        geo = sat.at(t)
        sub = wgs84.subpoint(geo)
        v = geo.velocity.km_per_s
        msg = {
            "schema_version": "1.0",
            "norad_id": norad_id,
            "name": name,
            "ts": iso,
            "lat": sub.latitude.degrees,
            "lon": sub.longitude.degrees,
            "alt_km": sub.elevation.km,
            "velocity_kms": float(np.sqrt((v ** 2).sum())),
        }
        producer.produce(POSITION_TOPIC, key=norad_id.encode(), value=json.dumps(msg).encode())
    producer.poll(0)


def main():
    consumer.subscribe([TLE_TOPIC])
    print(f"Propagator running. broker={BROKER} tick={TICK_S * 1000:.0f}ms", flush=True)
    last = time.monotonic()
    while True:
        msg = consumer.poll(0.2)
        if msg is not None and not msg.error():
            handle_tle(msg.value().decode())
        if time.monotonic() - last >= TICK_S:
            if sats:
                propagate_all()
            last = time.monotonic()


def shutdown(*_):
    consumer.close()
    producer.flush(5)
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

if __name__ == "__main__":
    main()