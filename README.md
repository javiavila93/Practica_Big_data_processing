# Proyecto Big Data Processing

En este ejercicio vamos a construir un caso de uso streaming + batch, similar a lo que haremos en la práctica del proyecto final.

## Fuente de datos

### Información de antena

Esta fuente de datos, es una fuente de datos en tiempo real en formato JSON, las distintas antenas de nuestra red enviaran mensajes con el siguiente schema:

| property  | description                     | data_type   |  example                                   |
|-----------|---------------------------------|-------------|--------------------------------------------|
| timestmap | Marca de tiempo en segundos     | LONG        | `1600528288`                               |
| id        | UUID de la antena               | STRING      | `"550e8400-e29b-41d4-a716-446655440000"`   |
| metric    | Nombre de la métrica            | STRING      | `"status"`, `"devices_count"`, `"battery"` |
| value     | Valor de la métrica             | INT         | `0`, `1`, `-1`, `90`                       |

En concreto la version 1.0.0 instaladas en el firmware de las antenas enviaran 3 tipos de mensaje:

#### Mensajes de estado

El campo `metric` tomara el valor de `status`.
El campo `value` tomara los siguientes valores:
* `0`: Antena activa y funcionando correctamente.
* `-1`: Antena activa y funcionando con errores.
* `1`: Antena desactivada.

```json
{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655440000", "metric": "status", "value": 0}
{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655441111", "metric": "status", "value": 1}
{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655442222", "metric": "status", "value": -1}
...
```

#### Mensajes de dispositivos conectados

El campo `metric` tomara el valor de `devices_count`.
El campo `value` llevara un total de los clientes conectados en el reporte.

```json
{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655440000", "metric": "devices_count", "value": 120}
{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655441111", "metric": "devices_count", "value": 30}
{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655442222", "metric": "devices_count", "value": 6}
...
```

#### Mensajes de batería

El campo `metric` tomara el valor de `battery`.
El campo `value` llevara un porcentaje del nivel de batería desde el `0` hasta el `100`.

```json
{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655440000", "metric": "battery", "value": 100}
{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655441111", "metric": "battery", "value": 70}
{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655442222", "metric": "battery", "value": 85}
...
```

### Metadatos de las antenas

Esta información se encontrará almacenada en una base de datos SQL.

| property   | description                      | data_type   |  example                                             |
|------------|----------------------------------|-------------|------------------------------------------------------|
| id         | UUID de la antena                | TEXT        | `"550e8400-e29b-41d4-a716-446655440000"`             |
| model      | Modelo de la antena              | TEXT        | `"CH-2020"`                                          |
| version    | Version de la antena             | TEXT        | `1.0.0`                                              |
| location   | Coordenadas: latitude, longitude | TEXT        | `39.831741, -3.915119`                               |

```sql
CREATE TABLE metadata (id TEXT, model TEXT, version TEXT, location TEXT);
```


## Servicio de monitorización

### REALTIME

* Recolecta las métricas de las antenas y son almacenadas en Apache Kafka en tiempo real.
* Spark Structured Streaming, hace métricas agregadas cada 5 minutos y guarda en PostgreSQL.
    * AVG, MAX, MIN (device_counts) por coordenadas GPS.

```sql
CREATE TABLE antenna_agg (location TEXT, date TIMESTAMP, avg_devices_count BIGINT, max_devices_count BIGINT, min_devices_count BIGINT);
```

* Spark Structured Streaming, también enviara los datos en formato PARQUET a un almacenamiento de google cloud storage, particionado por AÑO, MES, DIA, HORA.

### BATCH

* AVG, MAX, MIN (device_counts) por coordenadas GPS por hora.
* Numero de antenas con error agrupadas por modelo y versión.
* % de la hora que una antena ha estado activada, desactivada o con error por ID.

* **EXTRA:** Servicio de mantenimiento de datalake, que procesa los ficheros generados por el sistema de real-time y los agrega para compactar los ficheros y mejorar el rendimiento en las lecturas.


### Extra

Intentar visualizar las métricas que sean posibles en Superset.
