from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime

with DAG(
    "project1-workflow",
    start_date=datetime(2015, 12, 1),
    schedule_interval=None,
    # TODO Uruchamiając projekt, za każdym razem w konfiguracji uruchomienia Apache Airflow popraw ścieżki w parametrach dags_home i input_dir
    # TODO Zmień poniżej domyślne wartości parametrów classic_or_streaming oraz pig_or_hive na zgodne z Twoim projektem
    params={
      "dags_home": Param("/home/<nazwa-uzytkownika>/airflow/dags", type="string"),
      "bucket_name": Param("<nazwa-zasobnika>", type="string"),
    },
    render_template_as_native_obj=True
) as dag:

  load_fact_table = BashOperator(
    task_id="load_fact_table",
    bash_command=""" spark-submit --packages io.delta:delta-core_2.12:2.1.0 \
                                  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
                                  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
                                  --class UKTrafficAnalysis \
                                  --master yarn \
                                  --num-executors 10 \
                                  --driver-memory 2g \
                                  --executor-memory 2g \
                                  --executor-cores 2 \
                                  fact.jar {{ params.bucket_name }}"""
  )

  load_data_dim = BashOperator(
    task_id="load_data_dim",
    bash_command=""" spark-submit --packages io.delta:delta-core_2.12:2.1.0 \
                                  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
                                  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
                                  --class UKTrafficAnalysis \
                                  --master yarn \
                                  --num-executors 10 \
                                  --driver-memory 2g \
                                  --executor-memory 2g \
                                  --executor-cores 2 \
                                  dim_data.jar {{ params.bucket_name }}"""
  )

load_weather_dim = BashOperator(
    task_id="load_weather_dim",
    bash_command=""" spark-submit --packages io.delta:delta-core_2.12:2.1.0 \
                                  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
                                  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
                                  --class UKTrafficAnalysis \
                                  --master yarn \
                                  --num-executors 10 \
                                  --driver-memory 2g \
                                  --executor-memory 2g \
                                  --executor-cores 2 \
                                  dim_weather.jar {{ params.bucket_name }}"""
  )

load_authority_dim = BashOperator(
    task_id="load_authority_dim",
    bash_command=""" spark-submit --packages io.delta:delta-core_2.12:2.1.0 \
                                  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
                                  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
                                  --class UKTrafficAnalysis \
                                  --master yarn \
                                  --num-executors 10 \
                                  --driver-memory 2g \
                                  --executor-memory 2g \
                                  --executor-cores 2 \
                                  dim_authority.jar {{ params.bucket_name }}"""
  )

load_regions_dim = BashOperator(
    task_id="load_regions_dim",
    bash_command=""" spark-submit --packages io.delta:delta-core_2.12:2.1.0 \
                                  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
                                  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
                                  --class UKTrafficAnalysis \
                                  --master yarn \
                                  --num-executors 10 \
                                  --driver-memory 2g \
                                  --executor-memory 2g \
                                  --executor-cores 2 \
                                  dim_regions.jar {{ params.bucket_name }}"""
  )

load_time_dim = BashOperator(
    task_id="load_time_dim",
    bash_command=""" spark-submit --packages io.delta:delta-core_2.12:2.1.0 \
                                  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
                                  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
                                  --class UKTrafficAnalysis \
                                  --master yarn \
                                  --num-executors 10 \
                                  --driver-memory 2g \
                                  --executor-memory 2g \
                                  --executor-cores 2 \
                                  dim_time.jar {{ params.bucket_name }}"""
  )


load_weather_dim << load_authority_dim
load_time_dim << load_data_dim
load_fact_table << [load_regions_dim, load_authority_dim, load_data_dim]
