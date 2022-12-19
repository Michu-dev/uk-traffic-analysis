import org.apache.spark.sql._
import org.apache.spark.sql.functions._



object UKTrafficAnalysis {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("UKTrafficAnalysis")
//      .master("local[2]")
      .getOrCreate()

    val BUCKET_NAME = args(0)

    val mainDF = spark.read.option("header", true).csv(s"gs://${BUCKET_NAME}/uk-traffic/mainData*.csv")
      .withColumn("local_authority_ons_code", col("local_authoirty_ons_code"))
      .withColumn("timestamp_id", concat(date_format(col("count_date"), "yyyyMMdd"), col("hour")).cast("long"))

    val authDF = spark.read.option("header", true).csv(s"gs://${BUCKET_NAME}/uk-traffic/authorities*.csv")

    val regionsDF = spark.read.option("header", true).csv(s"gs://${BUCKET_NAME}/uk-traffic/regions*.csv")

    val weatherDF = spark.read.text(s"gs://${BUCKET_NAME}/uk-traffic/weather.txt")
      .select(split(col("value"), ": ").alias("value"))
      .select(split(col("value")(0), " ").alias("value"), col("value")(1).alias("weather_condition"))
      .select(col("value"), col("value")(4).alias("local_authority_ons_code"), to_timestamp(concat(col("value")(6), lit(" "), col("value")(8)), "dd/MM/yyyy HH:mm").alias("timestamp"), col("weather_condition"))
      .join(authDF)
      .drop(authDF("local_authority_ons_code"))
      .withColumn("weather_id", concat(date_format(to_timestamp(concat(col("value")(6), lit(" "), col("value")(8)), "dd/MM/yyyy HH:mm"), "yyyyMMddHH"), col("region_ons_code")))
      .select("weather_id", "weather_condition").dropDuplicates()

    val factTableDF = mainDF.join(authDF, mainDF("local_authority_ons_code") === authDF("local_authority_ons_code"))
      .drop(authDF("local_authority_ons_code"))
      .join(regionsDF, authDF("region_ons_code") === regionsDF("region_ons_code"))
      .withColumn("weather_id", concat(mainDF("timestamp_id"), regionsDF("region_ons_code")))
      .drop(authDF("region_ons_code"))
      .groupBy(col("region_id").cast("long").alias("region_id"), col("local_authority_id").cast("long").alias("local_authority_id"), col("timestamp_id").cast("long").alias("timestamp_id"), col("weather_id").alias("weather_id"), col("count_point_id").cast("long").alias("count_point_id"))
      .agg(sum(col("all_motor_vehicles")).alias("sum_all_motor_vehicles"), sum(col("all_hgvs")).alias("sum_all_hgvs")).na.fill(0, Array("sum_all_hgvs"))


    factTableDF.write.format("delta").mode("overwrite").save("/tmp/delta/fact")

    factTableDF.show(5)

    val dimWeatherDF = weatherDF
    dimWeatherDF.write.format("delta").mode("overwrite").save("/tmp/delta/dim_weather")

    val dimRegionsDF = regionsDF.select(col("region_id").cast("long"), col("region_name"), col("region_ons_code"))
    dimRegionsDF.write.format("delta").mode("overwrite").save("/tmp/delta/dim_regions")

    val dimAuthoritiesDF = authDF.select(col("local_authority_id").cast("long"), col("local_authority_ons_code"), col("local_authority_name"))
    dimAuthoritiesDF.write.format("delta").mode("overwrite").save("/tmp/delta/dim_authority")

    val dimDataDF = mainDF.select(col("count_point_id").cast("long"), col("direction_of_travel"), col("road_name"), col("road_type"), col("pedal_cycles"), col("two_wheeled_motor_vehicles"), col("cars_and_taxis"), col("buses_and_coaches"), col("lgvs"), col("all_hgvs"), col("all_motor_vehicles"))
    dimDataDF.write.format("delta").mode("overwrite").save("/tmp/delta/dim_data")

    val dimTimeDF = mainDF.select(col("timestamp_id").cast("long"))
      .withColumn("year", col("timestamp_id").substr(0,4).cast("int"))
      .withColumn("month", col("timestamp_id").substr(5,2).cast("int"))
      .withColumn("day", col("timestamp_id").substr(7,2).cast("int"))
      .withColumn("hour", col("timestamp_id").substr(9,2).cast("int"))

    dimTimeDF.write.format("delta").mode("overwrite").save("/tmp/delta/dim_time")

  }

}
