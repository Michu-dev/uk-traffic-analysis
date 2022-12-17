// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.ls("/FileStore/project2")

// COMMAND ----------

import org.apache.spark.sql._
import spark.implicits._

// COMMAND ----------

val mainDF = spark.read.option("header", true).csv("/FileStore/project2/mainDataScotland.csv").withColumn("local_authority_ons_code", col("local_authoirty_ons_code")).withColumn("timestamp_id", concat(date_format(col("count_date"), "yyyyMMdd"), col("hour")))
val authDF = spark.read.option("header", true).csv("/FileStore/project2/authoritiesScotland.csv")
val regionsDF = spark.read.option("header", true).csv("/FileStore/project2/regionsScotland.csv")
val weatherDF = spark.read.text("/FileStore/project2/weather.txt")
    .select(split(col("value"), ": ").alias("value"))
    .select(split(col("value")(0), " ").alias("value"), col("value")(1).alias("weather_condition"))
    .select(col("value"), col("value")(4).alias("local_authority_ons_code"), to_timestamp(concat(col("value")(6), lit(" "), col("value")(8)), "dd/MM/yyyy HH:mm").alias("timestamp"), col("weather_condition"))
    .join(authDF).drop(authDF("local_authority_ons_code"))
    .withColumn("weather_id", concat(date_format(to_timestamp(concat(col("value")(6), lit(" "), col("value")(8)), "dd/MM/yyyy HH:mm"), "yyyyMMddHH"), col("region_ons_code")))
//     .withColumn("timestamp_id", date_format(to_timestamp(concat(col("value")(6), lit(" "), col("value")(8)), "dd/MM/yyyy HH:mm"), "yyyyMMddHH"))
    .select("weather_id", "weather_condition").dropDuplicates()//, "region_ons_code").dropDuplicates()//, "timestamp_id")

// COMMAND ----------

val factTableDF = mainDF.join(authDF, mainDF("local_authority_ons_code") === authDF("local_authority_ons_code"))
                      .drop(authDF("local_authority_ons_code"))
                      .join(regionsDF, authDF("region_ons_code") === regionsDF("region_ons_code"))
                      .withColumn("weather_id", concat(mainDF("timestamp_id"), regionsDF("region_ons_code")))
                      .drop(authDF("region_ons_code"))
                      .groupBy("region_id", "local_authority_id", "timestamp_id", "weather_id", "count_point_id")
                      .agg(sum(col("all_motor_vehicles")).alias("sum(all_motor_vehicles)"), sum(col("all_hgvs")).alias("sum(all_hgvs)")).na.fill(0, Array("sum(all_hgvs)"))

// COMMAND ----------

display(factTableDF)

// COMMAND ----------


