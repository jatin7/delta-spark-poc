
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object Raw2DeltaApp extends App {

  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  extractAndLoad()

  private def extractAndLoad(): Unit = {
    val rawDf: DataFrame = spark.read
      .option("header", "true")
      .schema(Schemas.schema)
      .csv(Paths.raw)
      .withColumn("changed", when(col("updated").isNotNull, col("updated")).otherwise(col("created")))

    val deltaDf = rawDf
      .withColumn("rank", rank.over(Window.partitionBy("rut").orderBy(col("changed").desc)))
      .where(col("rank").equalTo(1))
      .select("rut", "value", "created", "updated")
      .sort("rut")

    deltaDf.write
      .format("delta")
      .option("overwriteSchema", "true")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(Paths.delta)
  }

}
