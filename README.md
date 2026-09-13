# Sattelite-radar
Mapa interactivo de satelites en tiempo real.
## Cómo arrancar

```bash
docker compose up -d          # levanta Kafka
bash create-topics.sh         # crea los topics(pending ejecutar solo en el arranque una vez definido los topics)
```

info útil:
a tener en cuenta: Screenshots, diagramas y logs son mucho más valiosos que un texto perfecto al final.

- Mapa: http://localhost:1880/worldmap

consumir de un topic:

docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic satellites.tle.raw --from-beginning --max-messages 1 \
  --property print.key=true --property key.separator=" => "

## Kafka

Se usa `confluentinc/cp-kafka:7.7.1` en modo KRaft (sin Zookeeper).

Se descartó `apache/kafka:3.9.0`: falla al arrancar con
`advertised.listeners cannot use ... 0.0.0.0` por un bug de esa versión
(KAFKA-18281, valida mal el listener del controller). La imagen de Confluent
7.7.1 va por dentro con Kafka 3.7, anterior al bug.

El broker expone dos direcciones:
- `localhost:9092` -> clientes del host (p. ej. la terminal).
- `kafka:19092` -> clientes en Docker (Node-RED).

Node-RED corre en contenedor, así que en el nodo de Kafka se pone `kafka:19092`
(con `localhost` apuntaría al propio contenedor de Node-RED). Ambos contenedores
deben estar en la misma red de Docker.

Comandos útiles:
```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic satellites.position --from-beginning
```

## Topics (pending completar)

| topic                  | para qué                                     |
|------------------------|----------------------------------------------|
| `satellites.tle.raw`   | datos orbitales crudos bajados de la API     |
| `satellites.position`  | posiciones calculadas (lat/lon)              |


## Node-RED

Plugins a instalar (Manage palette -> Install):
- node-red-contrib-web-worldmap
- node-red-contrib-kafkajs

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

La consulta se controla con dos parámetros. GROUP selecciona el conjunto de satélites a descargar y FORMAT el formato de salida. Una sola petición devuelve todos los satélites del grupo, así que no hay límite por parte de Celestrak en cuántos se descargan. Eso sí, no conviene consultar en bucle rápido: los TLE cambian aproximadamente una vez al día, con refrescar cada 6-12h basta, y un uso abusivo puede acabar en bloqueo de IP.

El grupo determina el volumen de satélites. stations (estaciones espaciales, ISS incluido) trae unos 10-20 y es el ideal para pruebas; galileo y gps-ops rondan los 30 cada uno; weather unos 50-60. starlink contiene miles de objetos y conviene evitarlo salvo que se filtre.

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

**pending: Contenedor aparte o conjuntamente con nodered?   buscar libreria de python?**



## Funcionalidades

| Cód. | Funcionalidad | Descripción | Tipo |
|------|---------------|-------------|------|
| C1 | Ingesta de TLE + eje pub/sub | Descarga periódica de TLE/OMM desde Celestrak y publicación satellites.tle.raw en Kafka (clave = NORAD ID, identificador unico por satelite). | Obligatoria |
| C2 | Propagación orbital en vivo | Consumer satellites.tle.raw que aplica SGP4 (satellite.js) para calcular lat/lon/altitud a ~1 Hz y publicarlas satellites.position. | Obligatoria |
| C3 | Mapa en tiempo real | Node-RED consume satellites.position y pinta iconos de satélites en worldmap, moviéndolos en directo. | Obligatoria |


***valorar/opcionales***
| C4 | Predicción de pases sobre Madrid | Cálculo de próximos pases (AOS/LOS, elevación máxima, hora) del ISS/otros sobre la estación terrena de Madrid. | opcional |
| C5 | Dashboard + endpoints HTTP | Panel con "próximo pase", contador de satélites y endpoint `GET /passes` de consulta (al estilo del `/bounds` del ejemplo de aviones). | Opcional |
| C6 | Trazado de órbita (ground-track) | Dibujo de la traza pasada + futura de un satélite seleccionado propagando N minutos por delante/detrás. | Opcional |


## Dudas y decisiones
meter node-red en el docker-compose --- problemas de red con kafka al no estar en la misma red.
Propagador en Contenedor aparte o conjuntamente con nodered?   
buscar libreria satellite.js de python o dejar todo homogenizado en javascript?
hace falta una bbdd distribuida?