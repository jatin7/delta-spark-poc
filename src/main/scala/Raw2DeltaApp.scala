
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object Raw2DeltaApp extends App {

  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  val schema = StructType(Seq(
    StructField("rut", StringType, nullable = false),
    StructField("value", IntegerType, nullable = false),
    StructField("created", TimestampType, nullable = false),
    StructField("updated", TimestampType, nullable = true),
    StructField("extractionDate", TimestampType, nullable = true),
  ))

  extractAndLoad()

  private def extractAndLoad(): Unit = {
    val rawDf: DataFrame = spark.read
      .option("header", "true")
      .schema(schema)
      .csv(Paths.raw)
      .withColumn("changed", when(
        col("updated").isNotNull,
        col("updated")
      ).otherwise(
        col("created")
      ))

    val deltaDf = rawDf
      .withColumn("rank", rank.over(Window.partitionBy("rut").orderBy(col("changed").desc)))
      .where(col("rank").equalTo(1))
      .select("rut", "value", "created", "updated")
      .sort("rut")
      .repartition(1)

    deltaDf.write
      .format("delta")
      .option("overwriteSchema", "true")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(Paths.delta)

    rawDf.show()
  }

}
