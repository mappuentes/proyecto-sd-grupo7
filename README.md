# Satellite Radar

Mapa interactivo de satélites en tiempo real a partir de datos orbitales de
CelesTrak. Kafka es el eje de comunicación publish/subscribe y Node-RED se
encarga de la ingesta, el cálculo orbital y la visualización.

## Estado actual

- Kafka funciona en modo KRaft y tiene almacenamiento persistente.
- Los topics `satellites.tle.raw`, `satellites.position` y `satellites.dlq` los crea
  automáticamente el servicio `kafka-init` al arrancar.
- Node-RED descarga el grupo `stations` de CelesTrak y publica un OMM por satélite.
- El propagador (contenedor aparte) consume las órbitas, aplica SGP4 y publica las
  posiciones cada `TICK_MS`.
- Node-RED consume `satellites.position` y mueve los marcadores reales en Worldmap.
- El arranque está coordinado para que el propagador y Node-RED no arranquen hasta que
  Kafka esté sano y los topics existan.

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
Propagador (contenedor Python): propagación SGP4
    │ latitud, longitud y altitud
    ▼
Kafka: satellites.position
    │ consumer group: map-view
    ▼
Node-RED: Worldmap y, posteriormente, globo 3D
```

Se mantiene Kafka como único eje pub/sub. MQTT, una base de datos distribuida y
Blockchain no son necesarios para la primera versión. La propagación se separa en
un contenedor propio por diseño de microservicios: cada componente tiene una única
responsabilidad (ingesta, cálculo, visualización) y se comunican solo por topics,
lo que permite desarrollarlos y desplegarlos por separado. El propagador se
implementa en Python con la librería Skyfield por comodidad de desarrollo; Kafka es
agnóstico al lenguaje, así que convive sin problema con el Node-RED en JavaScript.

## Cómo arrancar

```bash
docker compose up -d --build
```

Es el único comando necesario. Compose levanta los servicios en este orden:

```text
kafka (healthy)  ->  kafka-init (crea los topics y termina)  ->  propagator y nodered
```

- `kafka` tiene un healthcheck: solo se considera listo cuando el broker responde.
- `kafka-init` es un contenedor de un solo uso: crea los topics con su configuración
  (particiones, réplica, compactación, retención) y termina. En `docker compose ps`
  aparece como `Exited (0)`, que es lo correcto. Usa `--if-not-exists`, así que se
  puede repetir `up` sin duplicar ni romper nada.
- `propagator` y `nodered` esperan a que `kafka-init` termine con éxito
  (`service_completed_successfully`).

**Por qué existe `kafka-init`.** El healthcheck de Kafka solo comprueba que el broker
responde, no que los topics existan. Antes los topics se creaban con un script lanzado
a mano después de `up`, en paralelo al arranque de Node-RED. Si el consumidor de
Node-RED se suscribía a `satellites.position` antes de que existiera, kafkajs recibía
`This server does not host this topic-partition`, el nodo quedaba en error y no
reintentaba: el mapa salía vacío aunque el propagador publicase posiciones. Era una
condición de carrera (el resultado dependía de quién llegaba antes). Ahora el orden lo
garantiza Compose, no la suerte.

Para empezar desde cero (borra Kafka y el volumen de Node-RED de este proyecto):

```bash
docker compose down -v
docker compose up -d --build
```

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

Node-RED usa `kafka:19092` porque `localhost` apuntaría al propio contenedor.
Ambos servicios están en la misma red de Docker.

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

Consumer groups:

- `orbit-propagator`: reconstruye y mantiene el estado orbital.
- `map-view`: transforma posiciones al formato de Worldmap.

El productor actual usa `acks=all`. No se asumirá semántica exactly-once: los
consumidores deben tolerar duplicados usando NORAD ID, época orbital e instante
de cálculo.


## Node-RED

Dependencias instaladas:

- node-red-contrib-web-worldmap
- node-red-contrib-kafkajs

El flujo contiene dos recorridos desacoplados por Kafka:

1. consulta a CelesTrak y publicación de las órbitas en `satellites.tle.raw`;
2. consumo de posiciones y actualización de los marcadores de Worldmap.

La propagación SGP4 no se hace en Node-RED, sino en el contenedor propagador.

### Compartir cambios de Node-RED

El flujo compartido por Git está en `Node-Red/flows.json`. Los cambios hechos
directamente en ese archivo se incluyen en el siguiente commit.

Si se modifica el flujo desde el editor web de Node-RED, primero hay que pulsar
**Deploy** y copiar el flujo guardado en el contenedor al repositorio:

```bash
docker cp mynodered:/data/flows.json Node-Red/flows.json
```

Después se puede compartir todo el trabajo actual con:

```bash
git add README.md docker-compose.yml Node-Red/
git commit -m "añade Node-RED con Kafka y posicion de la ISS en tiempo real"
git push
```

El resto del equipo lo obtiene y arranca con:

```bash
git pull
docker compose up -d --build
```

El archivo `flows_cred.json` no se sube porque puede contener credenciales.

### Volumen de Node-RED (importante al actualizar)

Node-RED guarda su flujo y sus nodos en el volumen `node_red_data`, que Compose crea
y gestiona (ya no es `external`). Node-RED se construye ahora desde `Node-Red/Dockerfile`,
que instala los nodos de `package.json` y copia `flows.json`, así que un volumen nuevo y
vacío funciona desde el primer arranque sin pasos manuales.

Consecuencia para quien ya tenía un volumen `node_red_data` creado a mano: Compose ahora
lo nombra con el prefijo del proyecto (`<carpeta>_node_red_data`), por lo que **su volumen
antiguo deja de usarse** y verá Node-RED con el flujo del repo. Para no perder cambios
propios sin exportar, antes de hacer `git pull`:

```bash
docker cp mynodered:/data/flows.json Node-Red/flows.json
```

Un `flows.json` guardado en un volumen tiene prioridad sobre el del repo: el flujo del
repo solo se aplica cuando el volumen está vacío.

El volumen antiguo no se borra, solo deja de usarse (se ve con `docker volume ls`). Si
se olvidó exportar el flujo antes del `git pull`, se recupera con:

```bash
docker run --rm -v node_red_data:/data -v "$PWD":/out alpine cp /data/flows.json /out/flows-antiguo.json
```

## API: Celestrak

Celestrak no da la posición (lat/lon), da los "elementos orbitales": los
parámetros que describen la órbita (inclinación, excentricidad, etc.), con los que
se puede calcular dónde está el satélite en cualquier instante.

- TLE: el formato clásico, dos líneas de texto con números.
- OMM: la misma información en campos con nombre (JSON/XML), más cómodo de parsear.

Se pide OMM en JSON por comodidad; el contenido es equivalente al TLE.

Los datos se bajan de Celestrak (gratis, sin API key):

```
https://celestrak.org/NORAD/elements/gp.php?GROUP=stations&FORMAT=json
```

La consulta usa el grupo `stations` para descargar las estaciones (ISS y demás) y
publicar un evento por satélite. Se ejecuta al arrancar Node-RED y después cada 6
horas; también puede lanzarse a mano desde el nodo Inject. Si CelesTrak devuelve un
error o una respuesta inválida, el flujo muestra el error y no publica un evento
inválido.

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

## Propagación SGP4

Como la API da órbitas y no posiciones, hace falta "propagar" la órbita para sacar
la lat/lon del instante actual. Ese cálculo es el algoritmo SGP4, que implementa el
propagador con la librería **Skyfield** (Python).

    TLE/OMM  --(SGP4)-->  lat, lon, altitud  -->  al mapa

1. consume info orbital de los satelites
2. propaga con SGP4 (Skyfield)
3. publica posicion tiempo real satelites

El cálculo se hace en un contenedor propio (microservicio), separado de la ingesta
y la visualización. Se usa Python/Skyfield por comodidad de desarrollo; en
JavaScript el equivalente sería satellite.js. Kafka mantiene desacopladas las tres
etapas lógicas del flujo, así que el lenguaje del propagador es indiferente para el
resto del sistema.



## Funcionalidades

| Cód. | Funcionalidad | Descripción | Tipo |
|------|---------------|-------------|------|
| C1 | Ingesta de TLE + eje pub/sub | Descarga periódica de TLE/OMM desde Celestrak y publicación satellites.tle.raw en Kafka (clave = NORAD ID, identificador unico por satelite). | Obligatoria |
| C2 | Propagación orbital en vivo | El propagador consume satellites.tle.raw, aplica SGP4 (Skyfield) para calcular lat/lon/altitud y las publica en satellites.position. | Obligatoria |
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
- [x] Configurar explícitamente particiones, réplica y retención en el servicio `kafka-init`.
- [x] Crear los topics en el propio `docker compose up`, antes de arrancar los clientes.
- [ ] Verificar que otro integrante puede arrancar todo siguiendo este README.

**Criterio de cierre:** los dos comandos de arranque levantan Kafka y Node-RED, y
la ISS llega a `satellites.tle.raw` con NORAD ID como clave.

### Fase 2 - Recorrido funcional completo

- [x] Validar el estado HTTP, el JSON y los campos OMM recibidos.
- [x] Publicar un mensaje por cada satélite del grupo `stations`.
- [x] Consumir `satellites.tle.raw` con el grupo `orbit-propagator`.
- [x] Calcular latitud, longitud y altitud con `SGP4` en el propagador.
- [x] Publicar cada posición en `satellites.position`.
- [x] Consumir posiciones con `map-view` y mover los marcadores reales.
- [x] Desactivar el marcador fijo de demostración sin borrarlo todavía.

**Criterio de cierre:** los satélites de `stations` se mueven en Worldmap y todos
los datos del mapa han pasado por ambos topics.

### Fase 3 - Recuperación y errores

- [ ] Conservar la última descarga orbital válida y su fecha.
- [ ] Recuperar el estado orbital después de reiniciar el propagador.
- [ ] Rechazar eventos inválidos y mostrar el error.
- [ ] Ignorar órbitas antiguas y posiciones atrasadas del mismo NORAD ID.
- [ ] Mostrar cuándo los datos orbitales están desactualizados.
- [ ] Probar por separado la caída de CelesTrak, Node-RED y Kafka.

**Criterio de cierre:** reiniciar el propagador recupera el mapa y una caída externa
no provoca pérdida silenciosa ni consultas continuas a CelesTrak.

### Fase 4 - Globo 3D y entrega

- [ ] Añadir una vista 3D que consuma las mismas posiciones desde Kafka.
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
| `docker-compose.yml` | Incorporar Node-RED y el propagador, crear los topics con `kafka-init` y coordinar el arranque |
| `Node-Red/flows.json` | Completar ingesta y mapa |
| `propagator/` | Servicio de propagación SGP4 (Python) |
| `Node-Red/settings.js` | Configurar módulos y almacenamiento |
| `Node-Red/public/globe.html` | Añadir el globo 3D en la fase 4 |