# Block 2 - Propagator: consumes satellites.tle.raw, propagates with SGP4,
# publishes to satellites.position. Invalid messages go to satellites.dlq.
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import numpy as np
# OFFSET_BEGINNING Constante para leer desde el primer mensaje
from confluent_kafka import Consumer, Producer, OFFSET_BEGINNING, OFFSET_END
from sgp4.api import Satrec
from sgp4 import omm
from skyfield.api import EarthSatellite, load, wgs84

BROKER = os.getenv("KAFKA_BROKER", "kafka:19092")
TLE_TOPIC = os.getenv("TLE_TOPIC", "satellites.tle.raw")
POSITION_TOPIC = os.getenv("POSITION_TOPIC", "satellites.position")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "satellites.dlq")
GROUP_ID = os.getenv("GROUP_ID", "propagator")
TICK_S = float(os.getenv("TICK_MS", "1000")) / 1000.0

# Variables de entorno para el track de satélites
TRACK_REQUEST_TOPIC = os.getenv("TRACK_REQUEST_TOPIC", "satellites.track.request")
TRACK_TOPIC = os.getenv("TRACK_TOPIC", "satellites.track")
TRACK_HORIZON_SECONDS = int(os.getenv("TRACK_HORIZON_MINUTES", "60")) * 60
TRACK_STEP_SECONDS = int(os.getenv("TRACK_STEP_SECONDS", "60"))
TRACK_REFRESH_SECONDS = int(os.getenv("TRACK_REFRESH_SECONDS", "60"))
TRACK_LEASE_SECONDS = int(os.getenv("TRACK_LEASE_SECONDS", "90"))
MAX_ACTIVE_TRACKS = int(os.getenv("MAX_ACTIVE_TRACKS", "20"))

track_leases = {}  # norad_id -> lease_expiration_time
ts = load.timescale()
sats = {}  # norad_id -> (EarthSatellite, name, epoch)

consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": GROUP_ID,
    "enable.auto.commit": False, #Empezamos de 0 por si hay caída del propagador
})

# Función para leer desde el primer mensaje del topic en vez de con offset, que 
# dejaba el diccionario vacío si el propagador se caía y volvía a iniciar
def on_assign(c, partitions):
    for p in partitions:
        p.offset = OFFSET_BEGINNING if p.topic == TLE_TOPIC else OFFSET_END
    c.assign(partitions)

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
        norad_id = data["norad_id"]
        epoch = data.get("epoch","")
        known = sats.get(norad_id)

        if known and epoch <= known[2]:
            return  # ignore older or same epoch

        sats[norad_id] = (build_satellite(data), data.get("name", norad_id), epoch)
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
    for norad_id, (sat, name, _) in sats.items():
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

def compute_track(norad_id):
    sat, name, epoch = sats[norad_id]
    t0 = ts.now()
    # Es necesario trabajar en días porque Skyfield no permite offsets en segundos
    offsets_days = np.arange(0, TRACK_HORIZON_SECONDS + 1, TRACK_STEP_SECONDS) / 86400.0
    t = ts.tt_jd(t0.tt + offsets_days)     # 61 instantes de una vez (vectorizado)
    sub = wgs84.subpoint(sat.at(t))        # igual que en propagate_all, pero con arrays
    points = [
        [round(float(la), 4), round(float(lo), 4), round(float(al), 1)]
        for la, lo, al in zip(sub.latitude.degrees, sub.longitude.degrees, sub.elevation.km)
    ]
    return {
        "schema_version": "1.0",
        "norad_id": norad_id,
        "name": name,
        "epoch": epoch,
        "generated_at": t0.utc_iso(),
        "step_s": TRACK_STEP_SECONDS,
        "points": points,
    }


def publish_track(norad_id):
    track = compute_track(norad_id)
    producer.produce(TRACK_TOPIC, key=norad_id.encode(), value=json.dumps(track).encode())


def handle_track_request(raw):
    try:
        req = json.loads(raw)
        norad_id = str(req["norad_id"])
        force = bool(req.get("force"))
    except Exception as err:
        producer.produce(DLQ_TOPIC, json.dumps({
            "failed_stage": "track_request",
            "error": str(err),
            "original_payload": raw,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).encode())
        return
    if norad_id not in sats:
        return  # Sin órbita no hay track
    is_new = norad_id not in track_leases
    if is_new and len(track_leases) >= MAX_ACTIVE_TRACKS:
        return  # Limite de tracks activos alcanzado
    track_leases[norad_id] = time.monotonic() + TRACK_LEASE_SECONDS
    if is_new or force:
        publish_track(norad_id)  
        producer.poll(0)


def refresh_tracks():
    now = time.monotonic()
    for norad_id, expires in list(track_leases.items()):
        if expires < now or norad_id not in sats:
            del track_leases[norad_id]   # Se deja de calcular el track si nadie renueva
        else:
            publish_track(norad_id)
    producer.poll(0)



def main():
    consumer.subscribe([TLE_TOPIC, TRACK_REQUEST_TOPIC], on_assign=on_assign)
    print(f"Propagator running. broker={BROKER} tick={TICK_S * 1000:.0f}ms", flush=True)
    last = time.monotonic()
    last_track = time.monotonic()
    restored = False
    while True:
        msg = consumer.poll(0.2)
        if msg is None:
            if sats and not restored:
                print(f"Loaded {len(sats)} satellites from TLE topic", flush=True)
                restored = True
        elif not msg.error():
            if msg.topic() == TLE_TOPIC:
                handle_tle(msg.value().decode())
            else:
                handle_track_request(msg.value().decode())
        if time.monotonic() - last >= TICK_S:
            if sats:
                propagate_all()
            last = time.monotonic()
        if track_leases and time.monotonic() - last_track >= TRACK_REFRESH_SECONDS:
            refresh_tracks()
            last_track = time.monotonic()


def shutdown(*_):
    consumer.close()
    producer.flush(5)
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

if __name__ == "__main__":
    main()