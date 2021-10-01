package io.keepcoding.spark.exercise.batch

import java.time.OffsetDateTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object BatchJobAntenna extends BatchJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  import spark.implicits._
  // Función para leer el parquet del /temp generado con Streamingjob
  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(s"${storagePath}/data")
      //Filtramos por la variable de entorno FilterDate en Formato ISO 8001
      .filter(
        $"year" === filterDate.getYear &&
          $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
  }

  //Función para conectarse con el postgress y leer la tabla user_metadata
  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  //Función para realizar un right join entre DF parquet y DF user_metadata
  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    antennaDF.as("antenna")
      .join(
        metadataDF.as("metadata"),
        $"antenna.id" === $"metadata.id"
      ).drop($"metadata.id")
  }

  //Función para calcular el consumo a la hora de bytes según argumentos
  override def computeSumBytes(dataFrame: DataFrame, fieldID: String, valueLit: String): DataFrame = {
    dataFrame
      .select($"timestamp", col(fieldID), $"bytes")
      .groupBy(col(fieldID), window($"timestamp", "1 hour"))
      .agg(
        sum($"bytes").as("value")
      )
      .withColumn("type", lit(valueLit))
      .select($"window.start".as("timestamp"), col(fieldID).as("id"), $"value", $"type")

  }

  //Función para calcular la cantidad bytes utilizados y si supera la cuota por usuario
 override def calculateQuotaUser(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"email", $"bytes", $"timestamp", $"quota")
      .groupBy(window($"timestamp", "1 hour"), $"email", $"quota")
      .agg(
        sum($"bytes").as("usage")
      )
      .filter($"usage" > $"quota")
      .select($"email", $"usage", $"quota", $"window.start".as("timestamp"))

  }
  // Función para escribir en Postgress
  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }
  // Función para escribir en una ruta local en formato parquet
  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
    dataFrame
      .write
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(s"${storageRootPath}/historical")
  }

  def main(args: Array[String]): Unit = run(args)
}

