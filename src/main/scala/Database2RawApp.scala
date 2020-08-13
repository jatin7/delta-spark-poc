
import java.sql.Timestamp
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType, StringType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Database2RawApp extends App {

  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  Seq("2020-08-10", "2020-08-11", "2020-08-12").foreach(x => extractAndLoad(Timestamp.valueOf(s"$x 00:00:00")))

  val schema = StructType(Seq(
    StructField("rut", StringType, nullable = false),
    StructField("value", IntegerType, nullable = false),
    StructField("created", TimestampType, nullable = false),
    StructField("updated", TimestampType, nullable = true),
  ))

  private def extractAndLoad(date: Timestamp): Unit = {
    val dateTs = DateTimeFormatter.ISO_DATE.format(date.toLocalDateTime)
    spark.read
      .option("header", "true")
      .schema(schema)
      .csv(s"${Paths.database}/$dateTs-snapshot")
      .where(s"created = '$dateTs' OR updated = '$dateTs'")
      .withColumn("extractionDate", lit(dateTs))
      .write
      .partitionBy("rut")
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(s"${Paths.raw}/extractionDate=$dateTs")
  }

}
