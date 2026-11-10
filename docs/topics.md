# Topics de Kafka de la biblioteca

Los crea `kafka/topics.sh`, nunca el código (`auto.create.topics.enable=false` en los brokers). Se puede correr
las veces que haga falta: `--if-not-exists` no toca un topic que ya existe. Este archivo justifica, en una línea,
cada parámetro de cada topic.

| Topic | Key | Particiones | Réplicas | `min.insync.replicas` | `cleanup.policy` | Retención |
|---|---|---|---|---|---|---|
| `biblioteca.prestamos` | `usuarioSub` | 3 | 3 | 2 | `delete` | 7 días |
| `biblioteca.lecturas` | `libroId` | 3 | 3 | 2 | `delete` | 24 horas |
| `biblioteca.prestamos.dlt` | la del original | 3 | 3 | 2 | `delete` | 7 días |
| `biblioteca.lecturas.dlt` | la del original | 3 | 3 | 2 | `delete` | 7 días |
| `biblioteca.catalogo` | `libroId` | 3 | 3 | 2 | `compact` | sin plazo: queda el último de cada key |

## Las keys

- **`biblioteca.prestamos` con key `usuarioSub`:** todo lo de un lector cae en la misma partición y se lee en
  orden, así que su `prestamo.devuelto` llega siempre después de su `prestamo.creado` y `biblioteca-historial`
  encuentra el préstamo por `prestamoId` sin esperar ni reordenar. Por eso la devolución la publica
  `biblioteca-prestamos` con el `usuarioSub` del **dueño** del préstamo, no con el de quien la marca.
- **`biblioteca.lecturas` con key `libroId`:** las lecturas de un libro llegan en orden a un solo consumidor, que
  es lo que necesita la alerta por libro y ventana. El costo es la partición caliente: el libro `18` tiene el 40%
  de la noche y su partición (la 1) recibe 2.279 de 2.984 mensajes, cuatro veces más que la 0. Se acepta porque
  ninguna instancia procesa más de una partición por grupo y 2.279 mensajes por hora no la saturan; si un libro
  llegara a saturar una partición, la salida sería una key compuesta (`libroId` más `sesionId`) a costa de perder
  el orden por libro.
- **`biblioteca.catalogo` con key `libroId`:** la compactación guarda el último mensaje **de cada key**; con el
  libro como key, el último es el estado vigente de ese libro.
- **`.dlt` con la key del original:** así un mensaje desviado queda en la misma partición relativa y se puede
  reprocesar en orden (se usan en la E3).

## Particiones

- **3 en todos:** una por broker, y el máximo de instancias útiles por grupo: con 3 instancias de
  `biblioteca-eventos` cada una toma una partición; una cuarta queda sin nada (ver `docs/consumer-groups.md`).
  Más particiones no ayudan con 1 a 3 instancias y cada una cuesta archivos y memoria en los tres brokers.

## Réplicas, `min.insync.replicas=2` y `acks=all`

- **3 réplicas:** una copia en cada broker; se puede perder un broker sin perder datos ni dejar de escribir.
- **`min.insync.replicas=2`:** con `acks=all` el broker confirma solo cuando 2 copias tienen el mensaje; con un
  broker caído se sigue escribiendo, con dos caídos el productor recibe `NOT_ENOUGH_REPLICAS` en vez de una
  confirmación que viviría en una sola copia (medido en L9: 201 en 0,68 s con uno caído, no publica con dos).
- **`acks=all` en los productores** (`biblioteca-prestamos`, `biblioteca-libros`, el simulador): sin él, el
  `min.insync.replicas` no se aplica y un mensaje confirmado se puede perder con el líder.

## Retención y política de limpieza

- **`biblioteca.prestamos`, 7 días, `delete`:** es el que se rebobina (`biblioteca-historial`) y la historia que
  se puede reprocesar; una semana cubre un fin de semana largo sin que nadie mire y alcanza para rehacer
  `prestamos_semana` de la semana en curso.
- **`biblioteca.lecturas`, 24 horas, `delete`:** es telemetría de alto volumen (88% de los mensajes de la
  noche) y lo que importa queda agregado en `kpi_minuto` y `alertas`; un día alcanza para reprocesar la noche
  completa si un consumidor falla, sin guardar en tres brokers algo que nadie vuelve a leer.
- **`.dlt`, 7 días, `delete`:** el tiempo para que alguien revise un mensaje desviado y decida si se reprocesa;
  son pocos mensajes, así que el plazo largo no cuesta.
- **`biblioteca.catalogo`, `compact`, sin `retention.ms`:** no es una historia sino un estado; Kafka puede borrar
  las versiones viejas de un libro, pero nunca la última, así que un consumidor que lee desde el offset 0
  reconstruye el catálogo entero aunque hayan pasado meses. `retention.ms` no aplica a la compactación.
- **Los internos (`__consumer_offsets`) con 3 réplicas:** `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3` en el
  compose; con 1, perder un broker borraría los offsets confirmados de los grupos.

## Lo que muestra el clúster

Salida de `sh kafka/topics.sh` (los cinco temas con sus `Configs`; el orden de las líneas cambia de una vez a
otra):

```text
Created topic biblioteca.catalogo.
Topic: biblioteca.lecturas.dlt	TopicId: 2IQVDnQjQMqs2hwgiksZKQ	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2,cleanup.policy=delete,retention.ms=604800000
Topic: biblioteca.prestamos	TopicId: AVRvVvx8SwGCxKYMdIeSQQ	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2,cleanup.policy=delete,retention.ms=604800000
Topic: biblioteca.catalogo	TopicId: ZUrJ8sxBT12y8TNk0UbdAQ	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2,cleanup.policy=compact
Topic: biblioteca.lecturas	TopicId: aYCOJgEpQBiDzvecZnlgcg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2,cleanup.policy=delete,retention.ms=86400000
Topic: biblioteca.prestamos.dlt	TopicId: PM60BrQsRqGlYWzeVQT_Kg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2,cleanup.policy=delete,retention.ms=604800000
```

`biblioteca.catalogo` es el único con `cleanup.policy=compact` y el único sin `retention.ms`.
