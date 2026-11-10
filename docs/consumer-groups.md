# Consumer groups de la biblioteca

Todos viven en `biblioteca-eventos/src/streaming/`, uno por carpeta y por módulo de Nest. Cada grupo tiene dos
clases: la de Kafka (`<grupo>.consumidor.ts`, que hereda de `consumidor-kafka.ts`: conexión, suscripción y
commit) y la de negocio (`<grupo>.negocio.ts`: recibe el evento ya validado por `contrato.ts` y escribe en
Postgres). La clase de negocio no importa nada de Kafka.

| Consumer group | Lee | Escribe | Idempotencia |
|---|---|---|---|
| `biblioteca-panel` | `biblioteca.lecturas`, `biblioteca.prestamos` | `kpi_minuto` | Una fila por evento: `evento_id` es la clave primaria |
| `biblioteca-alertas` | `biblioteca.lecturas` | `ventanas_lectura`, `alertas` | `eventos_procesados (grupo, evento_id)` en la misma transacción que suma |
| `biblioteca-historial` | `biblioteca.prestamos` | `prestamos_semana`, `prestamos_vistos` | `eventos_procesados (grupo, evento_id)` en la misma transacción que suma |
| `biblioteca-catalogo` | `biblioteca.catalogo` | `catalogo_libros` | Se escribe el libro completo encima; no retrocede a un `emitidoEn` anterior |

Además, `biblioteca-libros` usa el grupo `biblioteca-libros` solo para leer su propio topic al arrancar, sin
confirmar offsets (ver `catalogo-kafka.mjs` en `biblioteca-libros`).

## El orden: escribir y después confirmar

```ts
for await (const m of flujo) {
  let evento;
  try {
    evento = this.contrato.validar(m);                       // JSON y DTO; si falla, lanza EventoInvalido
  } catch (error) {
    this.log.warn(`${donde} INVALIDO: ${error.message}`);    // en la E3, al .dlt
    await m.commit();                                        // reintentar no lo arregla
    continue;
  }
  const resultado = await this.procesarHastaEscribir(evento, donde); // INSERT; si es duplicado, no suma
  await m.commit();                                          // recién ahora
}
```

Commit manual después de escribir más idempotencia: entrega al menos una vez y resultado exactamente una vez. Si
el proceso muere entre el `INSERT` y el `commit`, al volver relee ese evento y el `INSERT` no suma. Un evento que
no cumple el contrato se registra y se confirma. Si lo que falla es Postgres, **no** se confirma: el mismo evento
se reintenta cada 2 segundos hasta que se pueda escribir (confirmarlo lo perdería).

## Por qué dos formas de idempotencia

- **`kpi_minuto`, una fila por evento.** El panel necesita contar por minuto y por libro, y la serie acumulada de
  lecturas en curso; con una fila por evento eso es un `GROUP BY` al leer, y el duplicado se descarta con
  `ON CONFLICT (evento_id) DO NOTHING`. Son 2.626 filas por noche: nada para Postgres.
- **`ventanas_lectura` y `prestamos_semana`, contadores.** La alerta se evalúa con cada lectura terminada y el
  historial asocia la devolución a su préstamo: los dos necesitan el total al momento, no al leer. El contador se
  suma en la misma transacción que registra `(grupo, evento_id)` en `eventos_procesados`; si el par ya estaba, la
  transacción no suma. La clave lleva el grupo porque el mismo evento lo procesan dos grupos.

## La regla de las alertas, y por qué un mínimo de 5

Por libro, en ventanas fijas de 5 minutos de `emitidoEn` (UTC; el panel las muestra en hora de Chile): alerta si
**más del 20%** de las lecturas terminadas fue `error` **y hubo al menos 5 terminadas**. Con cada terminada se
evalúa la ventana otra vez: si cumple, la alerta queda con las cifras al día; si deja de cumplir, se retira.

Sin mínimo, con el dataset de ejemplo también disparan dos ventanas que no son una racha:

| Libro | Ventana | En error / terminadas | Con mínimo de 5 |
|---|---|---|---|
| `18` | 21:05 a 21:10 | 1 / 3 (33,3%) | no dispara |
| **`10`** | **21:30 a 21:35** | **56 / 59 (94,9%)** | **dispara** |
| `16` | 21:50 a 21:55 | 1 / 3 (33,3%) | no dispara |

Con 4 o menos terminadas, **una sola** en error ya pasa el 20%: la alerta diría «falló una lectura», no «este
libro tiene un problema». La mediana es de 2 terminadas por ventana (187 ventanas con alguna terminada), y la
racha real tiene 59. Con 5, un error no basta (1 de 5 es 20%, no más): hacen falta 2. Un mínimo de 3 dejaría
pasar las dos ventanas de arriba.

## La devolución y su préstamo

`prestamo.devuelto` no dice en qué semana se prestó. `biblioteca-historial` guarda cada préstamo en
`prestamos_vistos` y, al llegar la devolución, lo busca por `prestamoId`, lo marca devuelto y resta un activo en la
semana y el libro **del préstamo**. Funciona sin esperas porque la key es `usuarioSub`: el préstamo y su
devolución caen en la misma partición y la devolución se lee después. Una segunda devolución del mismo préstamo
no resta otra vez. La semana es el lunes, en hora de Chile, de `emitidoEn` (2026-11-02 para toda la noche).

## Dos grupos sobre el mismo topic: cada uno con su offset

`biblioteca-panel` y `biblioteca-historial` leen `biblioteca.prestamos`. Con `eventos` detenido, se rebobinó solo
`biblioteca-historial`; `biblioteca-panel` siguió en el final, con lag 0 (salida real de este equipo, con una
reproducción en el topic):

```
$ kafka-consumer-groups.sh --bootstrap-server broker-1:19092 --describe --group biblioteca-panel
Consumer group 'biblioteca-panel' has no active members.

GROUP            TOPIC                PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
biblioteca-panel biblioteca.lecturas  0          492             492             0
biblioteca-panel biblioteca.lecturas  1          2279            2279            0
biblioteca-panel biblioteca.lecturas  2          213             213             0
biblioteca-panel biblioteca.prestamos 0          136             136             0
biblioteca-panel biblioteca.prestamos 1          145             145             0
biblioteca-panel biblioteca.prestamos 2          123             123             0
$ kafka-consumer-groups.sh --bootstrap-server broker-1:19092 --describe --group biblioteca-historial
Consumer group 'biblioteca-historial' has no active members.

GROUP                TOPIC                PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
biblioteca-historial biblioteca.prestamos 0          0               136             136
biblioteca-historial biblioteca.prestamos 1          0               145             145
biblioteca-historial biblioteca.prestamos 2          0               123             123
```

El mismo topic, dos grupos, dos offsets: `biblioteca-historial` en 0, con todo por leer, y `biblioteca-panel` en
el final, con lag 0. Rebobinar uno no movió al otro. Al levantar `eventos`, `biblioteca-historial` releyó los
404 mensajes (398 nuevos tras vaciar su tabla, 2 duplicados y 4 inválidos) y `prestamos_semana` quedó con las
mismas cifras (380 prestados, 18 devueltos, 362 activos); `biblioteca-panel` no leyó nada. Lo mismo pasa con
`biblioteca-panel` y `biblioteca-alertas` sobre `biblioteca.lecturas`: cada uno avanza con su propio
`CURRENT-OFFSET`.

## Las particiones se reparten dentro de un grupo

Con tres instancias de `biblioteca-eventos` (`docker compose run -d --no-deps --name eventos-2 eventos`, y
`eventos-3`), cada instancia toma una partición de cada topic; `biblioteca-panel`, que lee dos topics, toma dos:

```
$ kafka-consumer-groups.sh --bootstrap-server broker-1:19092 --describe --members --group biblioteca-alertas
GROUP              CONSUMER-ID                                             HOST          CLIENT-ID           #PARTITIONS
biblioteca-alertas biblioteca-eventos-0d7d5e4e-87d2-4051-a488-d58de69993e3 /172.18.0.20  biblioteca-eventos 1
biblioteca-alertas biblioteca-eventos-08d12202-cecf-4cb3-b9b5-8108e90c8822 /172.18.0.16  biblioteca-eventos 1
biblioteca-alertas biblioteca-eventos-f6a312b9-e722-4219-a401-b634067eed1f /172.18.0.19  biblioteca-eventos 1
$ kafka-consumer-groups.sh --bootstrap-server broker-1:19092 --describe --members --group biblioteca-panel
GROUP            CONSUMER-ID                                             HOST          CLIENT-ID           #PARTITIONS
biblioteca-panel biblioteca-eventos-1282078b-3734-4826-a396-c7f9432b46db /172.18.0.16  biblioteca-eventos 2
biblioteca-panel biblioteca-eventos-7d4dbd48-3575-4e0d-a9e4-6546d16902e0 /172.18.0.19  biblioteca-eventos 2
biblioteca-panel biblioteca-eventos-4cfe9143-77f3-47ff-9cc0-17db3dddd078 /172.18.0.20  biblioteca-eventos 2
```

Con cuatro (`eventos-4`), en los grupos que leen un solo topic de 3 particiones **una instancia queda sin
nada**; en `biblioteca-panel`, 6 particiones entre 4, queda 2, 2, 1 y 1:

```
$ kafka-consumer-groups.sh --bootstrap-server broker-1:19092 --describe --members --group biblioteca-alertas
GROUP              CONSUMER-ID                                             HOST          CLIENT-ID           #PARTITIONS
biblioteca-alertas biblioteca-eventos-0d7d5e4e-87d2-4051-a488-d58de69993e3 /172.18.0.20  biblioteca-eventos 1
biblioteca-alertas biblioteca-eventos-08d12202-cecf-4cb3-b9b5-8108e90c8822 /172.18.0.16  biblioteca-eventos 1
biblioteca-alertas biblioteca-eventos-f6a312b9-e722-4219-a401-b634067eed1f /172.18.0.19  biblioteca-eventos 1
biblioteca-alertas biblioteca-eventos-9bd6b77e-0cae-44e6-b0b5-b7c21e3da9ce /172.18.0.21  biblioteca-eventos 0
$ kafka-consumer-groups.sh --bootstrap-server broker-1:19092 --describe --members --group biblioteca-historial
GROUP                CONSUMER-ID                                             HOST          CLIENT-ID           #PARTITIONS
biblioteca-historial biblioteca-eventos-368879f2-1ec3-47bd-b4f9-1c5c96742c0b /172.18.0.20  biblioteca-eventos 1
biblioteca-historial biblioteca-eventos-11e8b68e-9238-43d2-b636-fa395e20ad1b /172.18.0.16  biblioteca-eventos 1
biblioteca-historial biblioteca-eventos-03c41029-432e-44d7-b686-c2d33387e233 /172.18.0.21  biblioteca-eventos 1
biblioteca-historial biblioteca-eventos-ae6d674c-fddf-446e-9a69-de853bc15d6c /172.18.0.19  biblioteca-eventos 0
$ kafka-consumer-groups.sh --bootstrap-server broker-1:19092 --describe --members --group biblioteca-panel
GROUP            CONSUMER-ID                                             HOST          CLIENT-ID           #PARTITIONS
biblioteca-panel biblioteca-eventos-1282078b-3734-4826-a396-c7f9432b46db /172.18.0.16  biblioteca-eventos 2
biblioteca-panel biblioteca-eventos-7d4dbd48-3575-4e0d-a9e4-6546d16902e0 /172.18.0.19  biblioteca-eventos 2
biblioteca-panel biblioteca-eventos-4cfe9143-77f3-47ff-9cc0-17db3dddd078 /172.18.0.20  biblioteca-eventos 1
biblioteca-panel biblioteca-eventos-a31dbddf-49a6-44b8-84b2-ecf360c8c0ec /172.18.0.21  biblioteca-eventos 1
```

Por eso 3 particiones: más instancias que particiones no leen más rápido, solo esperan. Al quitar las tres
instancias extra (`docker stop` y `docker rm`), la que queda vuelve a tomar las 3 particiones de cada topic
(`biblioteca-alertas` con un solo miembro y `#PARTITIONS 3`).

## Rebobinar y repetir

El procedimiento (detener `eventos`, `--reset-offsets --to-earliest`, levantar) y la limpieza para repetir una
demostración están en el `README.md`, sección «Kafka (L10)».
