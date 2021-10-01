# Proyecto Big Data Processing


## Localización de Entregas
- Caputaras de Pantalla en: 
	- ./Capturas/Tablas_SQL
	- ./Capturas/Superset***
- Datos Parquet {checkpoint, data y historical}  en ./temp_project



## Borrador Conceptual

Este proyecto funcion hibrido entre la cloud y el equipo local:

 - Servicios cloud: Kafka y Postgress
 - Servicios local: Docker Emisor de Señal y Spark Streaming para procesamiento.
 
### Proceso de Set-up

1. Levantamos una instancia VM en Google Cloud con una distribucción en ubuntu.

2. Dentro de la SSH de la instancia descargamos e instalamos Kafka

3. Configuramos el archivo /config/server-propierties para añadir la ip de publica de Kafka como entrada de escucha

4. Arrancamos el broker de zookeper y el de kafka

5. Modificamos las reglas de firewall en Google Cloud para aceptar la comunicación entrante del docker

5. Iniciamos el docker para la producción de datos con la ip publica de kafka para realizar la ingesta de datos (probar con el kafka-producer que funciona el listener de kafka)

6. Levantamos un postgresSQL en Google SQL

7. Creamos las tablas y aprovisionamos la tabla user_metada con el job JdbcProvisioner.

8. Ingesta Streaming gestionado con el Job AntenaStreaming

9. Trabajo Batch BatchJobAntenna: En este job se realiza el join user_metada para reducir la carga en streaming

10. Superset:
 - En la misma instancia de VM se instala Superset
 - Se configura la conexion con el progres
 - Se crean visualizaciones web con en la dirección de Superset
*** No he sido capaz de conectar el postgres, intentar dentro de poco

