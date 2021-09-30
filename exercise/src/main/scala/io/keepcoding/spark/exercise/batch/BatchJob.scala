package io.keepcoding.spark.exercise.batch

import java.sql.Timestamp
import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(year: Int, month: Int, day: Int, hour: Int, bytes: Long, timestamp: Timestamp, app: String, id: String, antenna_id: String)

trait BatchJob {

  val spark: SparkSession

  //Función que se encarga de leer de la ruta del Storage con filtro específico
  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame
  //Función que se encarga de leer la tabla de postgres user_metadata
  def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame
  //Función que se encarga de realizar cruce entre DF parquet y DF user_metadata
  def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  //Función que calcula el agregado de consumo de datos por usuario
  def computeSumBytesUser(dataFrame: DataFrame): DataFrame
  //Función que calcula el agregado de consumo de datos por app
  def computeSumBytesApp(dataFrame: DataFrame): DataFrame
  //Función que calcula el agregado de consumo de datos por Antena
  def computeSumBytesAntenna(dataFrame: DataFrame): DataFrame
  //Función que calcula el agregado de consumo de datos por usuario y muestra solo los que han pasado su límite
  def calculateQuotaUser(dataFrame: DataFrame): DataFrame
  //Función que escribe en el Postgress
  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit
  //Función que escribe en formato parquet en una ruta local leidos de kafka jerarquizados por AÑO, MES, DÍA DEL MES Y HORA
  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit

  //Variables de entorno
  def run(args: Array[String]): Unit = {
    val Array(filterDate, storagePath, jdbcUri, jdbcMetadataTable, aggJdbcTable, quotaTable, jdbcUser, jdbcPassword) = args
    println(s"Running with: ${args.toSeq}")

    // Variable que almacena el DF leídos de la ruta en local en formato parquet y los filtra por la variable filterDate
    val antennaDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))
    //Variable que almacena el DF de user_metadata
    val metadataDF = readAntennaMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    //Variable que almacena el DF del join entre DF parquet y user_metadata
    val antennaMetadataDF = enrichAntennaWithMetadata(antennaDF, metadataDF).cache()
    //Variable que almacena el DF con los datos agregados de consumo de bytes por usuario
    val SumBytesUserDF = computeSumBytesUser(antennaDF)
    //Variable que almacena el DF con los datos agregados de consumo de bytes por app
    val SumBytesAppDF = computeSumBytesApp(antennaDF)
    //Variable que almacena el DF con los datos agregados de consumo de bytes por antena
    val SumBytesAntennaDF = computeSumBytesAntenna(antennaDF)
    //Variable que almacena el DF con usuarios que superaron su quota
    val ConsumerUserQuota = calculateQuotaUser(antennaMetadataDF)

    writeToJdbc(SumBytesUserDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword) //Declaramos para escribir SumBytesUserDF en bytes_hourly
    writeToJdbc(SumBytesAppDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword) //Declaramos para escribir SumBytesAppDF en bytes_hourly
    writeToJdbc(SumBytesAntennaDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword) //Declaramos para escribir SumBytesAntennaDF en bytes_hourly
    writeToJdbc(ConsumerUserQuota, jdbcUri, quotaTable, jdbcUser, jdbcPassword)  //Declaramos para escribir ConsumerUserQuota  en user_quota_limit

    writeToStorage(antennaDF, storagePath) //Declaramos para escribir en el histórico en /storagePath/historical

    spark.close()
  }

}
