import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object Schemas {

  val schema = StructType(Seq(
    StructField("rut", StringType, nullable = false),
    StructField("value", IntegerType, nullable = false),
    StructField("created", TimestampType, nullable = false),
    StructField("updated", TimestampType, nullable = true),
  ))

}
