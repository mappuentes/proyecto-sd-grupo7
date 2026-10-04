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
from sgp4.api import Satrec, SatrecArray
from sgp4 import omm
from skyfield.api import EarthSatellite, load, wgs84
from skyfield.constants import AU_KM, DAY_S
from skyfield.functions import mxv
from skyfield.positionlib import Geocentric
from skyfield.sgp4lib import TEME

BROKER = os.getenv("KAFKA_BROKER", "kafka1:19092,kafka2:19092,kafka3:19092")
TLE_TOPIC = os.getenv("TLE_TOPIC", "satellites.tle.raw")
POSITION_TOPIC = os.getenv("POSITION_TOPIC", "satellites.position")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "satellites.dlq")
GROUP_ID = os.getenv("GROUP_ID", "propagator")
TICK_S = float(os.getenv("TICK_MS", "1000")) / 1000.0

# Variables de entorno para el track de satélites
TRACK_REQUEST_TOPIC = os.getenv("TRACK_REQUEST_TOPIC", "satellites.track.request")
TRACK_TOPIC = os.getenv("TRACK_TOPIC", "satellites.track")
# El horizonte de la trayectoria se adapta al periodo de cada órbita (ver compute_track)
TRACK_ORBITS = float(os.getenv("TRACK_ORBITS", "1"))              # órbitas completas a dibujar
TRACK_POINTS = int(os.getenv("TRACK_POINTS", "120"))              # puntos por trayectoria, sea cual sea la órbita
TRACK_MAX_HORIZON_S = float(os.getenv("TRACK_MAX_HORIZON_MINUTES", "1440")) * 60   # tope: 24 h
TRACK_FALLBACK_HORIZON_S = 3600.0                                 # si el periodo no es válido
# Sistema de referencia de la trayectoria:
#   inertial -> la órbita real en el espacio, con la Tierra congelada en el instante actual
#               (elipse cerrada; es lo que se espera ver al dibujar una órbita completa)
#   ground   -> posiciones relativas a la Tierra que gira (traza sobre el terreno elevada);
#               con órbitas largas se deforma en bucles porque la Tierra gira debajo
TRACK_FRAME = os.getenv("TRACK_FRAME", "inertial").lower()
TRACK_REFRESH_SECONDS = int(os.getenv("TRACK_REFRESH_SECONDS", "60"))
TRACK_LEASE_SECONDS = int(os.getenv("TRACK_LEASE_SECONDS", "90"))
MAX_ACTIVE_TRACKS = int(os.getenv("MAX_ACTIVE_TRACKS", "20"))

track_leases = {}  # norad_id -> lease_expiration_time
ts = load.timescale(builtin=True)  # tablas de tiempo incluidas: no descarga nada (demo sin Internet)
sats = {}  # norad_id -> (EarthSatellite, name, epoch, groups: set[str])
batch = None  # (norad_ids, names, groups, SatrecArray); se rehace cuando cambia sats

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


def produce(topic, value, key=None):
    # Si la cola local de Kafka se llena, vaciamos un poco y reintentamos en vez de caernos
    while True:
        try:
            producer.produce(topic, key=key, value=value)
            return
        except BufferError:
            producer.poll(0.5)


def send_to_dlq(stage, err, raw):
    produce(DLQ_TOPIC, json.dumps({
        "failed_stage": stage,
        "error": str(err),
        "original_payload": raw,
        "ts": datetime.now(timezone.utc).isoformat(),
    }).encode())


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
    global batch
    try:
        data = json.loads(raw)
        norad_id = data["norad_id"]
        epoch = data.get("epoch", "")
        group = data.get("group")
        known = sats.get(norad_id)

        # Un satélite puede estar en varios grupos (la ISS está en stations y en visual).
        # El grupo se apunta aunque el TLE sea repetido, porque llega con la misma epoch.
        groups = known[3] if known else set()
        if group and group not in groups:
            groups.add(group)
            batch = None

        if known and epoch <= known[2]:
            return  # ignore older or same epoch

        sats[norad_id] = (build_satellite(data), data.get("name", norad_id), epoch, groups)
        batch = None
    except Exception as err:
        send_to_dlq("parse/satrec", err, raw)


def get_batch():
    global batch
    if batch is None:
        ids = list(sats)
        batch = (
            ids,
            [sats[i][1] for i in ids],
            [sorted(sats[i][3]) for i in ids],
            SatrecArray([sats[i][0].model for i in ids]),
        )
    return batch


def propagate_all():
    # Mismo cálculo que sat.at(t) de Skyfield, pero para todos los satélites a la vez:
    # SGP4 en C sobre un SatrecArray y una sola rotación TEME -> GCRS
    ids, names, groups, sat_array = get_batch()
    t = ts.now()
    iso = t.utc_iso()
    jd = np.array([t.whole])
    # Fracción UTC igual que EarthSatellite (usa API privada de Skyfield: fijar su versión)
    fr = np.array([t.tai_fraction - t._leap_seconds() / DAY_S])
    err, r, v = sat_array.sgp4(jd, fr)  # r, v: (n, 1, 3) en km y km/s, marco TEME
    ok = err[:, 0] == 0  # SGP4 falla en órbitas decaídas; se omiten en vez de publicar NaN
    r_gcrs = mxv(TEME.rotation_at(t).T, r[:, 0, :].T / AU_KM)
    geo = wgs84.geographic_position_of(Geocentric(r_gcrs, None, t))
    lats, lons, alts = geo.latitude.degrees, geo.longitude.degrees, geo.elevation.km
    speeds = np.linalg.norm(v[:, 0, :], axis=1)  # la rotación no cambia el módulo
    for i in np.flatnonzero(ok):
        msg = {
            "schema_version": "1.0",
            "norad_id": ids[i],
            "name": names[i],
            "groups": groups[i],
            "ts": iso,
            "lat": float(lats[i]),
            "lon": float(lons[i]),
            "alt_km": float(alts[i]),
            "velocity_kms": float(speeds[i]),
        }
        produce(POSITION_TOPIC, json.dumps(msg).encode(), key=ids[i].encode())
    producer.poll(0)


def orbital_period_s(sat):
    # no_kozai es el movimiento medio del TLE en radianes por minuto: una vuelta son 2π
    n = sat.model.no_kozai
    if not np.isfinite(n) or n <= 0:
        return None
    return 2 * np.pi / n * 60.0


def track_horizon_s(sat):
    # Una órbita completa (o TRACK_ORBITS) en vez de una hora fija:
    #   LEO (ISS ~92 min): una vuelta entera, no dos tercios.
    #   MEO (GPS ~12 h):   la órbita completa, no un arco corto.
    #   GEO (~24 h):       el día completo; casi no se mueve sobre el suelo, así que la
    #                      traza se queda en un punto o un "ocho" pequeño, que es lo real.
    # El tope evita trayectorias enormes con órbitas muy altas.
    period = orbital_period_s(sat)
    if period is None:
        return TRACK_FALLBACK_HORIZON_S, None
    return min(period * TRACK_ORBITS, TRACK_MAX_HORIZON_S), period


def compute_track(norad_id, t0=None):
    sat, name, epoch, _ = sats[norad_id]
    if t0 is None:          # (t0 se puede fijar en pruebas)
        t0 = ts.now()
    horizon_s, period_s = track_horizon_s(sat)
    # Mismo número de puntos para cualquier órbita: el paso sale solo
    # (~46 s en la ISS, ~6 min en GPS, ~12 min en GEO) y el mensaje tiene siempre el mismo tamaño.
    offsets_s = np.linspace(0.0, horizon_s, TRACK_POINTS)
    # Skyfield trabaja en días, no en segundos
    t = ts.tt_jd(t0.tt + offsets_s / 86400.0)
    positions = sat.at(t)                  # posiciones en el espacio (GCRS), una por instante
    if TRACK_FRAME != "ground":
        # Todas se pasan a lat/lon con la orientación de la Tierra en t0, no en su propio instante:
        # así no se mezcla la rotación terrestre con la órbita y sale la elipse real, cerrada.
        frozen = ts.tt_jd(np.full(len(offsets_s), t0.tt))
        positions = Geocentric(positions.position.au, None, frozen)
    geo = wgs84.geographic_position_of(positions)
    points = [
        [round(float(la), 4), round(float(lo), 4), round(float(al), 1)]
        for la, lo, al in zip(geo.latitude.degrees, geo.longitude.degrees, geo.elevation.km)
        if np.isfinite(la) and np.isfinite(lo) and np.isfinite(al)   # SGP4 da NaN si la órbita decae
    ]
    return {
        "schema_version": "1.1",
        "norad_id": norad_id,
        "name": name,
        "epoch": epoch,
        "generated_at": t0.utc_iso(),
        "period_min": round(period_s / 60, 1) if period_s else None,
        "horizon_s": round(horizon_s),
        "step_s": round(horizon_s / (TRACK_POINTS - 1), 1),
        "frame": "ground" if TRACK_FRAME == "ground" else "inertial",
        "points": points,
    }


def publish_track(norad_id):
    track = compute_track(norad_id)
    produce(TRACK_TOPIC, json.dumps(track).encode(), key=norad_id.encode())


def handle_track_request(raw):
    try:
        req = json.loads(raw)
        norad_id = str(req["norad_id"])
        force = bool(req.get("force"))
    except Exception as err:
        send_to_dlq("track_request", err, raw)
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