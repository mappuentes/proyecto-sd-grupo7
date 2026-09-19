# Satellite Radar

Mapa interactivo de satélites en tiempo real a partir de datos orbitales de
CelesTrak. Kafka es el eje de comunicación publish/subscribe y Node-RED se
encarga de la ingesta, el cálculo orbital y la visualización.

## Estado actual

- Kafka funciona en modo KRaft y tiene almacenamiento persistente.
- Existen los topics `satellites.tle.raw` y `satellites.position`.
- Node-RED consulta la ISS al arrancar y cada 6 horas, y publica su OMM en Kafka.
- Node-RED calcula la posición de la ISS cada segundo con SGP4.
- La posición pasa por `satellites.position` antes de llegar a Worldmap.
- Node-RED se construye con dependencias fijadas y espera a que Kafka esté listo.
- Falta calcular posiciones reales y consumirlas en el mapa.

## Arquitectura propuesta

```text
CelesTrak
    │ HTTP cada 6 horas
    ▼
Node-RED: ingesta y validación
    │ un OMM por satélite
    ▼
Kafka: satellites.tle.raw
    │ consumer group: orbit-propagator
    ▼
Node-RED + satellite.js: propagación SGP4 cada segundo
    │ latitud, longitud y altitud
    ▼
Kafka: satellites.position
    │ consumer group: map-view
    ▼
Node-RED: Worldmap y, posteriormente, globo 3D
```

Se mantiene Kafka como único eje pub/sub. MQTT, una base de datos distribuida y
un servicio Python no son necesarios para la primera versión. El propagador se
implementará dentro de Node-RED con `satellite.js`; solo se separará en otro
contenedor si aparece una necesidad real de escalado o mantenimiento.

## Cómo arrancar

```bash
docker compose up -d --build
bash create-topics.sh
```

Compose levanta Kafka y Node-RED en la misma red. El healthcheck de Kafka evita
que Node-RED arranque antes de que el broker acepte conexiones. El script de
topics es idempotente y debe ejecutarse después de arrancar los servicios.

- Mapa: http://localhost:1880/worldmap

consumir de un topic:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic satellites.tle.raw --from-beginning --max-messages 1 \
  --property print.key=true --property key.separator=" => "
```

## Kafka

Se usa `confluentinc/cp-kafka:7.7.1` en modo KRaft (sin Zookeeper).

Se descartó `apache/kafka:3.9.0`: falla al arrancar con
`advertised.listeners cannot use ... 0.0.0.0` por un bug de esa versión
(KAFKA-18281, valida mal el listener del controller). La imagen de Confluent
7.7.1 va por dentro con Kafka 3.7, anterior al bug.

El broker expone dos direcciones:
- `localhost:9092` -> clientes del host (p. ej. la terminal).
- `kafka:19092` -> clientes en Docker (Node-RED).

Cuando Node-RED se incorpore a Compose, usará `kafka:19092` porque `localhost`
apuntaría al propio contenedor. Ambos servicios estarán en la misma red de Docker.

Comandos útiles:
```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic satellites.position --from-beginning
```

## Topics y consumidores

| Topic | Contenido | Clave | Particiones | Réplicas | Conservación |
|---|---|---|---:|---:|---|
| `satellites.tle.raw` | Último OMM conocido | NORAD ID | 1 | 1 | Compactación por clave |
| `satellites.position` | Posiciones calculadas | NORAD ID | 1 | 1 | Retención inicial de 1 hora |

Una partición es suficiente para el grupo `stations` y mantiene un orden simple.
El factor de replicación es 1 porque solo hay un broker; esta demo no ofrece alta
disponibilidad ante la caída del broker.

Consumer groups previstos:

- `orbit-propagator`: reconstruye y mantiene el estado orbital.
- `map-view`: transforma posiciones al formato de Worldmap.

El productor actual usa `acks=all`. No se asumirá semántica exactly-once: los
consumidores deben tolerar duplicados usando NORAD ID, época orbital e instante
de cálculo.


## Node-RED

Dependencias previstas:

- node-red-contrib-web-worldmap
- node-red-contrib-kafkajs
- satellite.js

El flujo contiene tres recorridos desacoplados por Kafka:

1. consulta y publicación de la órbita de la ISS;
2. consumo de la órbita, propagación SGP4 y publicación de posiciones;
3. consumo de posiciones y actualización del marcador de Worldmap.

## API: Celestrak

Celestrak no da la posición (lat/lon), da los "elementos orbitales": los
parámetros que describen la órbita (inclinación, excentricidad, etc.), con los que
se puede calcular dónde está el satélite en cualquier instante.

- TLE: el formato clásico, dos líneas de texto con números.
- OMM: la misma información en campos con nombre (JSON/XML), más cómodo de parsear.

Se pide OMM en JSON por comodidad; el contenido es equivalente al TLE.

Los datos se bajan de Celestrak (gratis, sin API key):

```
https://celestrak.org/NORAD/elements/gp.php?CATNR=25544&FORMAT=json
```

La consulta actual usa el NORAD ID `25544` para descargar únicamente la ISS. Se
ejecuta al arrancar Node-RED y después cada 6 horas; también puede lanzarse a
mano desde el nodo Inject. Si CelesTrak devuelve un error o una respuesta sin la
ISS, el flujo muestra el error y no publica un evento inválido.

En la fase 2 se cambiará a `GROUP=stations` para publicar un evento por satélite.
Este grupo pequeño es adecuado para desarrollo antes de probar grupos mayores.

### Por qué Celestrak y no una API de posición

Existen APIs que dan la lat/lon ya calculada (wheretheiss.at sin key, o N2YO con
key), pero se descartan como fuente principal por dos motivos:

1. Devuelven un satélite por petición. Para cientos de satélites refrescando cada
   segundo son cientos de peticiones por segundo -> se revientan los límites y la
   demo depende de que el servicio externo esté disponible.
2. Consumir posiciones ya calculadas convierte el proyecto en un mero proxy de lo
   que calcula otro.

Con Celestrak se baja el conjunto de TLE en una sola petición (cada varias horas)
y las posiciones se calculan en local, a cualquier frecuencia, sin límites y hasta
sin conexión. Además, propagar la órbita es lo que aporta valor técnico y habilita
las funciones "inteligentes" (predicción de pases, geofencing).

## satellite.js

Como la API da órbitas y no posiciones, hace falta "propagar" la órbita para sacar
la lat/lon del instante actual. Ese cálculo es el algoritmo SGP4, y la librería
que lo implementa en JS es **satellite.js**.

    TLE/OMM  --(satellite.js / SGP4)-->  lat, lon, altitud  -->  al mapa

1. consume info orbital de los satelites
2. propaga con satellite.js
3. publica posicion tiempo real satelites

El cálculo se hará inicialmente dentro de Node-RED. Esto reutiliza JavaScript,
reduce contenedores y mantiene Kafka entre las tres etapas lógicas del flujo.



## Funcionalidades

| Cód. | Funcionalidad | Descripción | Tipo |
|------|---------------|-------------|------|
| C1 | Ingesta de TLE + eje pub/sub | Descarga periódica de TLE/OMM desde Celestrak y publicación satellites.tle.raw en Kafka (clave = NORAD ID, identificador unico por satelite). | Obligatoria |
| C2 | Propagación orbital en vivo | Consumer satellites.tle.raw que aplica SGP4 (satellite.js) para calcular lat/lon/altitud a ~1 Hz y publicarlas satellites.position. | Obligatoria |
| C3 | Mapa en tiempo real | Node-RED consume satellites.position y pinta iconos de satélites en worldmap, moviéndolos en directo. | Obligatoria |


Funcionalidades opcionales:

| Cód. | Funcionalidad | Descripción | Tipo |
|------|---------------|-------------|------|
| C4 | Predicción de pases sobre Madrid | Cálculo de próximos pases (AOS/LOS, elevación máxima, hora) del ISS/otros sobre la estación terrena de Madrid. | opcional |
| C5 | Dashboard + endpoints HTTP | Panel con "próximo pase", contador de satélites y endpoint `GET /passes` de consulta (al estilo del `/bounds` del ejemplo de aviones). | Opcional |
| C6 | Trazado de órbita (ground-track) | Dibujo de la traza pasada + futura de un satélite seleccionado propagando N minutos por delante/detrás. | Opcional |


## Plan de implementación

### Fase 1 - Arranque reproducible

- [x] Añadir Node-RED al `docker-compose.yml` en la misma red que Kafka.
- [x] Crear `Node-Red/package.json`, lockfile y Dockerfile con versiones fijadas.
- [x] Esperar a que Kafka esté disponible antes de iniciar los consumidores.
- [x] Configurar explícitamente particiones, réplica y retención en `create-topics.sh`.
- [ ] Verificar que otro integrante puede arrancar todo siguiendo este README.

**Criterio de cierre:** los dos comandos de arranque levantan Kafka y Node-RED, y
la ISS llega a `satellites.tle.raw` con NORAD ID como clave.

### Fase 2 - Recorrido funcional completo

- [ ] Validar el estado HTTP, el JSON y los campos OMM recibidos.
- [ ] Publicar un mensaje por cada satélite del grupo `stations`.
- [x] Consumir `satellites.tle.raw` con el grupo `orbit-propagator`.
- [x] Calcular latitud, longitud y altitud cada segundo con `satellite.js`.
- [x] Publicar cada posición en `satellites.position`.
- [x] Consumir posiciones con `map-view` y mover los marcadores reales.
- [x] Desactivar el marcador fijo de demostración sin borrarlo todavía.

**Criterio de cierre:** los satélites de `stations` se mueven en Worldmap y todos
los datos del mapa han pasado por ambos topics.

### Fase 3 - Recuperación y errores

- [ ] Conservar la última descarga orbital válida y su fecha.
- [ ] Recuperar el estado orbital después de reiniciar Node-RED.
- [ ] Rechazar eventos inválidos y mostrar el error en Node-RED.
- [ ] Ignorar órbitas antiguas y posiciones atrasadas del mismo NORAD ID.
- [ ] Mostrar cuándo los datos orbitales están desactualizados.
- [ ] Probar por separado la caída de CelesTrak, Node-RED y Kafka.

**Criterio de cierre:** reiniciar Node-RED recupera el mapa y una caída externa no
provoca pérdida silenciosa ni consultas continuas a CelesTrak.

### Fase 4 - Globo 3D y entrega

- [ ] Añadir una vista 3D que consuma las mismas posiciones desde Node-RED.
- [ ] Mostrar nombre, NORAD ID, altitud y fecha al seleccionar un satélite.
- [ ] Preparar capturas, diagrama, logs y exportación PDF del flujo.
- [ ] Documentar topics, particiones, réplica, grupos y semántica de entrega.
- [ ] Ensayar una demo de cinco minutos, incluido un fallo controlado.

**Criterio de cierre:** la demo se repite desde un entorno limpio y el equipo
puede justificar las decisiones exigidas por el enunciado.

## Fuera del alcance inicial

Predicción de pases, trazado de órbitas, más grupos de satélites, base de datos
distribuida, MQTT, Blockchain y aprendizaje federado quedan aplazados hasta que
el recorrido obligatorio funcione de extremo a extremo.

## Archivos previstos

| Archivo | Motivo |
|---|---|
| `docker-compose.yml` | Incorporar Node-RED y coordinar el arranque |
| `create-topics.sh` | Fijar la configuración de los topics |
| `Node-Red/flows.json` | Completar ingesta, propagación y mapa |
| `Node-Red/package.json` | Declarar dependencias reproducibles |
| `Node-Red/package-lock.json` | Fijar las versiones resueltas |
| `Node-Red/Dockerfile` | Construir la instancia de Node-RED |
| `Node-Red/settings.js` | Configurar módulos y almacenamiento |
| `Node-Red/public/globe.html` | Añadir el globo 3D en la fase 4 |
