
import java.sql.Timestamp

import org.apache.spark.sql.types.{StringType, StructField, StructType, _}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Database2RawApp extends App {

  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  Seq("2020-08-10", "2020-08-11", "2020-08-12").map(extract)


  private def extract(date: String) = {

    val ts = Timestamp.valueOf(s"$date 00:00:00") // TODO: It can be a parameter

    spark.read
      .option("header", "true")
      .schema(Schemas.schema)
      .csv(s"${Paths.database}/$date-snapshot")
      .where(s"created = '$ts' OR updated = '$ts'")
      .write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(s"${Paths.raw}/extracted=$date")
  }

}
