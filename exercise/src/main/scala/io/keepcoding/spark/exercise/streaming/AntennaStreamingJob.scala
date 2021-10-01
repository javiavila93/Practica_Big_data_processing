package io.keepcoding.spark.exercise.streaming

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, types}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}

object AntennaStreamingJob extends StreamingJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark Proyect Streaming")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream // Iniciamos la lectura
      .format("kafka") //Introducimos el formato a leer
      .option("kafka.bootstrap.servers", kafkaServer) // Indicamos el modo de consumo y la dirección
      .option("subscribe", topic) //Indicamos el modo de conexion (consumidor) y el topic por el cual consumir
      .load() //Cargamos los datos para trabajar

  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
  //Definimos el esquema utilizando el case class definido en BatchJob Scala
   val antennaMessageSchema: StructType = ScalaReflection.schemaFor[AntennaMessage].dataType.asInstanceOf[StructType]

    // Definimos el modo de lectura de los datos como json y su esquema
    dataFrame
      .select(from_json(col("value").cast(StringType), antennaMessageSchema).as("json"))
      .select("json.*") //Desencapsulamos el json
      .withColumn("timestamp", $"timestamp".cast(TimestampType)) //Transformamos de String a Timestamp



  }

  //Función para sumar los bytes según los argumentos proporcionados en StreamingJob
  override def computeSumBytes(dataFrame: DataFrame,  fieldID: String, valueLit: String): DataFrame = {
    dataFrame
      .select($"timestamp", col(fieldID), $"bytes") //Seleccionamos la columnas con las que trabajar
      .withWatermark("timestamp", "1 minutes") //Marca de 1 minuto
      .groupBy(col(fieldID), window($"timestamp", "5 minutes")) // Definimos la ventana de 5 minutos
      .agg(
        sum($"bytes").as("value") //Realizamos la agregación y cambiamos el nombre por "value"
      )
      .withColumn("type", lit(valueLit))
      .select($"window.start".as("timestamp"), col(fieldID).as("id"), $"value", $"type") //Seleccionamos el inicio de la venta y los valores subir al postgres
  }


  // Función para escribir en Postgress
  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
    .writeStream
    .foreachBatch { (data: DataFrame, batchId: Long) =>
    data
    .write
    .mode(SaveMode.Append) //Modo Append que permite llamar en distintas funciones si perdida de dato (ver StreamingJob.scala)
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", jdbcURI)
    .option("dbtable", "bytes")
    .option("user", user)
    .option("password", password)
    .save()
  }
    .start()
    .awaitTermination()
  }
  // Función para escribir en una ruta local
  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {

   //Creamos una variable por año, mes, día del mes y hora según el timestamp para enviar al Storage
    val columns = dataFrame.columns.map(col).toSeq ++
      Seq(
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour")
      )

    //Escribimos en el formato parquet en la ruta definida
    dataFrame
      .select(columns: _*)
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .option("path", s"${storageRootPath}/data")
      .option("checkpointLocation", s"${storageRootPath}/checkpoint")
      .start()
      .awaitTermination()

  }
  def main(args: Array[String]): Unit = run(args)

}
