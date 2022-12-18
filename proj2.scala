// Databricks notebook source
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import spark.implicits._

// COMMAND ----------

// Read and transform source data

val mainDF = spark.read.option("header", true).csv("/FileStore/project2/mainData*.csv")
                .withColumn("local_authority_ons_code", col("local_authoirty_ons_code"))
                .withColumn("timestamp_id", concat(date_format(col("count_date"), "yyyyMMdd"), col("hour")).cast("long"))
val authDF = spark.read.option("header", true).csv("/FileStore/project2/authorities*.csv")
val regionsDF = spark.read.option("header", true).csv("/FileStore/project2/regions*.csv")
val weatherDF = spark.read.text("/FileStore/project2/weather.txt")
    .select(split(col("value"), ": ").alias("value"))
    .select(split(col("value")(0), " ").alias("value"), col("value")(1).alias("weather_condition"))
    .select(col("value"), col("value")(4).alias("local_authority_ons_code"), to_timestamp(concat(col("value")(6), lit(" "), col("value")(8)), "dd/MM/yyyy HH:mm").alias("timestamp"), col("weather_condition"))
    .join(authDF)
    .drop(authDF("local_authority_ons_code"))
    .withColumn("weather_id", concat(date_format(to_timestamp(concat(col("value")(6), lit(" "), col("value")(8)), "dd/MM/yyyy HH:mm"), "yyyyMMddHH"), col("region_ons_code")))
    .select("weather_id", "weather_condition").dropDuplicates()

// COMMAND ----------

val factTableDF = mainDF.join(authDF, mainDF("local_authority_ons_code") === authDF("local_authority_ons_code"))
                      .drop(authDF("local_authority_ons_code"))
                      .join(regionsDF, authDF("region_ons_code") === regionsDF("region_ons_code"))
                      .withColumn("weather_id", concat(mainDF("timestamp_id"), regionsDF("region_ons_code")))
                      .drop(authDF("region_ons_code"))
                      .groupBy($"region_id".cast("long").alias("region_id"), $"local_authority_id".cast("long").alias("local_authority_id"), $"timestamp_id".cast("long").alias("timestamp_id"), $"weather_id".alias("weather_id"), $"count_point_id".cast("long").alias("count_point_id"))
                      .agg(sum(col("all_motor_vehicles")).alias("sum_all_motor_vehicles"), sum(col("all_hgvs")).alias("sum_all_hgvs")).na.fill(0, Array("sum_all_hgvs"))

factTableDF.write.format("delta").mode("overwrite").save("/tmp/delta/fact")

// COMMAND ----------

val dimWeatherDF = weatherDF
dimWeatherDF.write.format("delta").mode("overwrite").save("/tmp/delta/dim_weather")

// COMMAND ----------

val dimRegionsDF = regionsDF.select(col("region_id").cast("long"), $"region_name", $"region_ons_code")
dimRegionsDF.write.format("delta").mode("overwrite").save("/tmp/delta/dim_regions")

// COMMAND ----------

val dimAuthoritiesDF = authDF.select(col("local_authority_id").cast("long"), $"local_authority_ons_code", $"local_authority_name")
dimAuthoritiesDF.write.format("delta").mode("overwrite").save("/tmp/delta/dim_authority")

// COMMAND ----------

val dimDataDF = mainDF.select(col("count_point_id").cast("long"), $"direction_of_travel", $"road_name", $"road_type", $"pedal_cycles", $"two_wheeled_motor_vehicles", $"cars_and_taxis", $"buses_and_coaches", $"lgvs", $"all_hgvs", $"all_motor_vehicles")
dimDataDF.write.format("delta").mode("overwrite").save("/tmp/delta/dim_data")

// COMMAND ----------

val dimTimeDF = mainDF.select(col("timestamp_id").cast("long"))
                      .withColumn("year", col("timestamp_id").substr(0,4).cast("int"))
                      .withColumn("month", col("timestamp_id").substr(5,2).cast("int"))
                      .withColumn("day", col("timestamp_id").substr(7,2).cast("int"))
                      .withColumn("hour", col("timestamp_id").substr(9,2).cast("int"))
dimTimeDF.write.format("delta").mode("overwrite").save("/tmp/delta/dim_time")

// COMMAND ----------


