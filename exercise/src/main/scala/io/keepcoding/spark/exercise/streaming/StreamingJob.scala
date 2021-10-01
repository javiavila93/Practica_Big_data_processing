package io.keepcoding.spark.exercise.streaming

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(bytes: Long, timestamp: Timestamp, app: String, id: String, antenna_id: String) //Definimos el case class de los datos recibir

trait StreamingJob {

  val spark: SparkSession

  //Función que se encarga de leer de kafka
  def readFromKafka(kafkaServer: String, topic: String): DataFrame
  //Función encargada transformar el mensaje de kafka en un dataframe por columnas(keys y esquema de AntennaMessage)
  def parserJsonData(dataFrame: DataFrame): DataFrame
  //Función que calcula el consumo de consumo de bytes segun los argumentos cada 5 min y añade la columna type con el literal
  def computeSumBytes(dataFrame: DataFrame, fieldID: String, valuelit: String): DataFrame
  //Función que escribe en el Postgress los datos de computeSumBytesUser, computeSumBytesApp y computeSumBytesAntenna
  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]
  //Función que escribe en formato parquet en una ruta local leidos de kafka jerarquizados por AÑO, MES, DÍA DEL MES Y HORA
  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]
  //Variables de entorno
  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword, storageRootPath) = args
    println(s"Running with: ${args.toSeq}")

    // Variable que almacena el DF leídos de kafak
    val kafkaDF = readFromKafka(kafkaServer, topic)
    //Variable que almacena el DF con los datos transformados
    val antennaDF = parserJsonData(kafkaDF)
    //Variable del futuro en ejecución para escribir los datos en parquet
    val storageFuture = writeToStorage(antennaDF, storageRootPath)
    //Variable que almacena el DF con los datos agregados de consumo de bytes por usuario
    val aggBySumBytesDFUser = computeSumBytes(antennaDF, "id", "user_total_bytes")
    //Variable que almacena el DF con los datos agregados de consumo de bytes por app
    val aggBySumBytesDFApp = computeSumBytes(antennaDF, "app", "app_bytes_total")
    //Variable que almacena el DF con los datos agregados de consumo de bytes por antena
    val aggBySumBytesDFAntenna = computeSumBytes(antennaDF, "antenna_id", "antenna_bytes_total")
    //Variable del futuro en ejecución para escribir los datos en postgres de aggBySumBytesDFUser
    val aggFutureUser = writeToJdbc(aggBySumBytesDFUser, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    //Variable del futuro en ejecución para escribir los datos en postgres de aggBySumBytesDFApp
    val aggFutureAPP = writeToJdbc(aggBySumBytesDFApp, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    //Variable del futuro en ejecución para escribir los datos en postgres de aggBySumBytesDFAntenna
    val aggFutureAntenna = writeToJdbc(aggBySumBytesDFAntenna, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)


    //Funcion para mantener el Spark operativo hasta finalizar el job (Infito) del array secuencial
    Await.result(Future.sequence(Seq(aggFutureUser, aggFutureAPP, aggFutureAntenna, storageFuture)), Duration.Inf)

    spark.close()
  }

}
