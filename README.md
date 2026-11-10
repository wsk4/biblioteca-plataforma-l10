# biblioteca-plataforma

Levanta el sistema entero —Postgres, RabbitMQ en clúster, Kafka con tres brokers y los siete servicios:
`biblioteca-eventos`, `biblioteca-libros`, `biblioteca-prestamos`, `biblioteca-admin`, `biblioteca-bff`,
`biblioteca-gateway` y `biblioteca-web`— con **un solo comando**: `bash levantar.sh`. Este repositorio no tiene
código de aplicación: solo `compose.yml`, `levantar.sh`, `.env.example`, `kafka/`, `deploy/`, `docs/` y `rabbitmq/`.

| Servicio | Repositorio | Puerto |
|---|---|---|
| `web` | `biblioteca-web` | 4200 |
| `gateway` | `biblioteca-gateway` | 8080 |
| `bff` | `biblioteca-bff` | 3000 |
| `libros` | `biblioteca-libros` | 3001 |
| `prestamos` | `biblioteca-prestamos` | 3002 |
| `eventos` | `biblioteca-eventos` | 3010 |
| `admin` | `biblioteca-admin` | 3020 |
| `postgres` / `rabbit1`, `rabbit2` | (imágenes oficiales) | 5432 / 5672, 5673 y 15672, 15673 |
| `controller-1` a `controller-3`, `broker-1` a `broker-3` | `apache/kafka:4.4.0` | 9092, 9094 y 9096 (brokers) |
| `kafka-ui` | `ghcr.io/kafbat/kafka-ui:v1.5.0` | 8085 |

## Cómo clonar los ocho y levantar

Los ocho repositorios van **uno al lado del otro**, en la misma carpeta, con el nombre **sin** el sufijo `-l10`
(el `compose.yml` busca `../biblioteca-<pieza>`). La lista con los enlaces está en `docs/repositorios.md`. Tras hacer
Fork de cada uno, y parado en la carpeta donde quieras tenerlos, los mismos comandos sirven en Windows
(PowerShell), macOS y Linux:

```bash
cd $HOME/DSY1107
mkdir l10
cd l10
git clone https://github.com/TU_USUARIO/biblioteca-plataforma-l10.git biblioteca-plataforma
git clone https://github.com/TU_USUARIO/biblioteca-web-l10.git biblioteca-web
git clone https://github.com/TU_USUARIO/biblioteca-gateway-l10.git biblioteca-gateway
git clone https://github.com/TU_USUARIO/biblioteca-bff-l10.git biblioteca-bff
git clone https://github.com/TU_USUARIO/biblioteca-libros-l10.git biblioteca-libros
git clone https://github.com/TU_USUARIO/biblioteca-prestamos-l10.git biblioteca-prestamos
git clone https://github.com/TU_USUARIO/biblioteca-eventos-l10.git biblioteca-eventos
git clone https://github.com/TU_USUARIO/biblioteca-admin-l10.git biblioteca-admin

# la herramienta que publica la noche de la Feria (no es de los ocho: se clona, no se forkea)
git clone https://github.com/Umbingelelo/biblioteca-simulador.git biblioteca-simulador
```

Cada repositorio con variables trae un `.env.example`, con los nombres y sin los valores. `bash levantar.sh` copia
cada `.env.example` a `.env` donde falte y se detiene si queda algún valor en blanco. También puedes hacerlo a mano
(Windows: `Copy-Item .env.example .env`; macOS y Linux: `cp .env.example .env`) y rellenarlos en **estos seis**:
`biblioteca-plataforma`, `biblioteca-gateway`, `biblioteca-bff`, `biblioteca-prestamos`, `biblioteca-eventos` y
`biblioteca-admin`. `biblioteca-libros` y `biblioteca-web` no leen ningún `.env` al levantar con Docker.

Después, desde `biblioteca-plataforma`, **un comando**:

```bash
bash levantar.sh
```

Hace, en este orden: crea el `.env` de cada repositorio desde su `.env.example` donde falte (y se detiene
diciendo qué valores en blanco rellenar), levanta los tres brokers de Kafka, crea los topics con
`sh kafka/topics.sh`, construye y levanta todo lo demás con `docker compose up -d --build --wait`, y comprueba
con `curl` que cada servicio responde. Los topics se crean **antes** que los servicios porque `biblioteca-eventos`
lee `biblioteca.prestamos` y `biblioteca.lecturas` al arrancar y el clúster no crea topics al vuelo.
Al final deja `docker compose ps` y dice cómo comprobar: Kafka UI en http://localhost:8085.
Para bajarlo sin borrar los datos: `docker compose down`.

Si cambias un repositorio y quieres reconstruir solo ese servicio: `docker compose up -d --build <servicio>`
(`gateway`, `bff`, `prestamos`, ...).

## Kafka (L9)

Tres controllers y tres brokers en modo KRaft, más Kafka UI. Los bloques `x-…` del principio del `compose.yml`
son las anclas que usan los siete servicios. Desde el equipo se usa
`KAFKA_BROKERS=localhost:9092,localhost:9094,localhost:9096`; desde un contenedor de la misma red,
`KAFKA_BROKERS=broker-1:19092,broker-2:19092,broker-3:19092` (el compose se lo pone a `prestamos` y a `eventos`).

| Topic | Quién publica | Key | Particiones · réplicas | Retención |
|---|---|---|---|---|
| `biblioteca.prestamos` | `biblioteca-prestamos` (y el simulador) | `usuarioSub` | 3 · 3 | 7 días |
| `biblioteca.lecturas` | el simulador (los lectores electrónicos) | `libroId` | 3 · 3 | 24 horas |
| `biblioteca.prestamos.dlt` y `biblioteca.lecturas.dlt` | (se usan en la E3) | | 3 · 3 | 7 días |

Todos con `min.insync.replicas=2` y `cleanup.policy=delete`. Los crea **solo** `kafka/topics.sh`, que se puede
correr las veces que haga falta (`--if-not-exists`); los `WARNING` sobre puntos y guiones bajos son normales.
Para ver el líder y el ISR de cada partición:

```bash
export MSYS_NO_PATHCONV=1   # solo en Git Bash
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker-1:19092 --describe --topic biblioteca.prestamos
# Partition: 0  Leader: 4  Replicas: 4,5,6  Isr: 4,5,6   (y lo mismo con 5 y 6)
```

Para ver qué quedó escrito, con key y headers:

```bash
docker compose exec -T broker-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server broker-1:19092 --topic biblioteca.prestamos --from-beginning --timeout-ms 8000 \
  --property print.key=true --property print.headers=true
```

El simulador (`biblioteca-simulador`, clonado al lado) publica la noche de la Feria con
`node reproducir.mjs --velocidad 60`. Con un broker detenido (`docker compose stop broker-3`) se sigue
publicando; con dos detenidos el clúster rechaza la escritura (`acks=all` y `min.insync.replicas=2`).
`docker compose up -d --wait` los devuelve.

Más: `docs/red.md` (el diseño de las dos nubes) y `deploy/aws/verificar-permisos.sh` (la prueba de permisos del
Learner Lab, que se corre desde esta carpeta con `bash deploy/aws/verificar-permisos.sh`).

## Kafka (L10): los consumer groups, el panel en vivo y el catálogo

`biblioteca-eventos` tiene **cuatro consumer groups**, cada uno en su carpeta y su módulo de Nest dentro de
`src/streaming/`. Todos confirman el offset **a mano y después de escribir** en Postgres, y son idempotentes: un
evento que llega dos veces no suma dos veces. El detalle está en `docs/consumer-groups.md`; los parámetros de cada
topic, en `docs/topics.md`.

| Consumer group | Lee | Escribe | Para qué |
|---|---|---|---|
| `biblioteca-panel` | `biblioteca.lecturas` y `biblioteca.prestamos` | `kpi_minuto` (una fila por evento) | El panel en vivo: por minuto y libro, lecturas en curso y préstamos |
| `biblioteca-alertas` | `biblioteca.lecturas` | `ventanas_lectura` y `alertas` | Más del 20% de lecturas terminadas en error, por libro, en ventanas de 5 minutos, con al menos 5 terminadas |
| `biblioteca-historial` | `biblioteca.prestamos` | `prestamos_semana` | Por semana y libro: prestados, devueltos y activos. Es el que se rebobina |
| `biblioteca-catalogo` | `biblioteca.catalogo` (compactado) | `catalogo_libros` | El último estado de cada libro, reconstruido desde el offset 0 en cada arranque |

`biblioteca-libros` publica su catálogo en `biblioteca.catalogo` (`libro.publicado` y `libro.actualizado`, key
`libroId`, `cleanup.policy=compact`): al arrancar lee lo que ya está en el topic y publica solo lo que falta o
cambió, y revisa `datos/catalogo.json` cada 5 segundos.

**El panel en vivo.** Angular (`/panel`) pide `GET /v1/panel/en-vivo` al gateway cada 5 segundos; el gateway lo
pasa al BFF, que exige el grupo `bibliotecarios` (el administrador de la biblioteca; con otro, **403**) y se lo
pide a `biblioteca-eventos` (`GET /panel/en-vivo`), el dueño de `kpi_minuto` y `alertas`. El BFF nunca se conecta a
Postgres. Con el simulador a `--velocidad 60`, el panel avanza unos cinco minutos de la noche por refresco.

Para ver los grupos y su lag:

```bash
export MSYS_NO_PATHCONV=1   # solo en Git Bash
docker compose exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server broker-1:19092 --describe --group biblioteca-panel
```

**Rebobinar `biblioteca-historial`.** Se detiene el grupo (está dentro de `eventos`), se lleva su offset al
principio y se levanta otra vez. Con el grupo vivo la herramienta se niega (`... the current state is Stable`):

```bash
docker compose stop eventos
docker compose exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server broker-1:19092 --group biblioteca-historial \
  --topic biblioteca.prestamos --reset-offsets --to-earliest --execute     # NEW-OFFSET 0 en las tres
docker compose up -d --wait eventos
```

Como es idempotente, `prestamos_semana` queda con las mismas cifras. Para verla **rehecha** desde cero, antes de
levantar `eventos` se vacía lo que escribe ese grupo:

```bash
docker compose exec postgres psql -U biblioteca -d biblioteca -c "TRUNCATE prestamos_semana, prestamos_vistos" \
  -c "DELETE FROM eventos_procesados WHERE grupo = 'biblioteca-historial'"
```

**Para repetir una demostración** (el panel, la alerta, el rebobinado): una segunda reproducción del mismo dataset
no mueve nada, porque es entera de duplicados. Se vacían las tablas de lectura y se reproduce otra vez; la
historia en Kafka no se toca:

```bash
docker compose exec postgres psql -U biblioteca -d biblioteca -c \
  "TRUNCATE kpi_minuto, ventanas_lectura, alertas, prestamos_semana, prestamos_vistos, eventos_procesados"
cd ../biblioteca-simulador && node reproducir.mjs --velocidad 60
```

**Tres y cuatro instancias de `eventos`** (para `docs/consumer-groups.md`). Las extra se levantan con
`docker compose run`, que no publica el puerto 3010; en un equipo de 8 GB, con Kafka UI detenido:

```bash
docker compose stop kafka-ui
docker compose run -d --no-deps --name eventos-2 eventos
docker compose run -d --no-deps --name eventos-3 eventos     # tres instancias: 1, 1 y 1
docker compose run -d --no-deps --name eventos-4 eventos     # cuatro: una queda con 0 particiones
docker compose exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server broker-1:19092 --describe --members --group biblioteca-alertas
docker stop eventos-2 eventos-3 eventos-4 && docker rm eventos-2 eventos-3 eventos-4
docker compose start kafka-ui
```

## El clúster de RabbitMQ (L8)

```
   biblioteca-admin :3020 ──AMQP──> :5672  rabbit1 ═══ rabbit2  :5673
                          ──HTTP──> :15672                      :15673
   biblioteca-eventos :3010 ──────> :5672 rabbit1,  :5432 Postgres
```

Dos nodos, `rabbit@rabbit1` y `rabbit@rabbit2`, con la misma cookie de Erlang (`RABBITMQ_COOKIE` del
`.env`). El clúster lo arma `rabbitmq/cluster.conf`, montado en los dos nodos: en una máquina limpia
un solo `docker compose up -d --wait` deja los dos unidos. Las seis colas son quorum.

Procedimiento de levantamiento desde un clon limpio, en `biblioteca-plataforma`:

```bash
bash levantar.sh
docker compose exec rabbit1 rabbitmqctl cluster_status
docker compose exec rabbit1 rabbitmqctl import_definitions /etc/rabbitmq/politicas.json
```

`cluster_status` tiene que traer los dos nodos en `Running Nodes`. Las políticas viven en
`rabbitmq/politicas.json` y están explicadas en `docs/politicas.md`; las métricas que se vigilan, en
`docs/metricas.md`.

Si se recrea `rabbit1` (por ejemplo al cambiar sus volúmenes), el worker y `prestamos` quedan
colgados de la conexión vieja: `docker compose restart eventos prestamos`.

## La API de administración (`biblioteca-admin`, puerto 3020)

Todas exigen un access token del grupo `bibliotecarios`.

| Verbo y ruta | Qué hace |
|---|---|
| `GET /v1/admin/colas` | Lista las colas (API HTTP de management) |
| `POST /v1/admin/colas` | Crea una cola; quorum por omisión |
| `DELETE /v1/admin/colas/:nombre` | Borra una cola |
| `GET /v1/admin/exchanges` | Lista los exchanges |
| `POST /v1/admin/exchanges` | Crea un exchange `direct`, `topic`, `fanout` o `headers` |
| `DELETE /v1/admin/exchanges/:nombre` | Borra un exchange |
| `POST /v1/admin/bindings` | Crea un binding cola + exchange + routing key |
| `DELETE /v1/admin/bindings` | Borra un binding, con los tres datos en el cuerpo |

## Retención de las DLQ

Elegimos `reject-publish` también en las DLQ: si una DLQ llega a mil mensajes el sistema lleva rato
roto, y preferimos que se note fuerte. Lo que se pierde: el mensaje que iba a morir número 1001 no
llega a la DLQ y no queda en `mensajes_muertos`. Con `drop-head` se perderían en silencio los más
viejos.

## Mensaje envenenado

Desde `biblioteca-plataforma`, con el worker corriendo:

```bash
docker compose run --rm --no-deps eventos node herramientas/emitir.mjs 1 --roto
```

Qué se tiene que ver: dos `ERROR ... hacia la DLQ` en `docker compose logs eventos` (auditoria y
notificaciones) y `docker compose exec rabbit1 rabbitmqctl list_queues name messages` con
`auditoria.dlq 1` y `notificaciones.dlq 1`.
