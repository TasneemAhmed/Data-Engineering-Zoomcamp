import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types

"""
argparse used to incorporate the parsing of command line arguments. 
Instead of having to manually set variables inside of the code,
argparse can be used to add flexibility and reusability to your code by allowing user input values to be parsed and utilized.
"""
import argparse
"""
    while create Spark session, remove .master("spark://localhost:7077")
    to make it if one need to set dynamically by and also if want to allocate resources as CPU cores & RAM:
    spark-submit --master "spark://localhost:7077" pyspark_sql.py

    or because submit job by Data proc which by default create master(like Yarn and allocate resources)
"""
def main(params):

    green_src = params.green_path
    yellow_src = params.yellow_path
    zones_src = params.zones_path
    joined_output = params.output_path

    spark = SparkSession.builder\
        .appName('spark-sql') \
        .getOrCreate()


    green_schema = types.StructType([
        types.StructField("VendorID", types.IntegerType(), True),
        types.StructField("lpep_pickup_datetime", types.TimestampType(), True),
        types.StructField("lpep_dropoff_datetime", types.TimestampType(), True),
        types.StructField("store_and_fwd_flag", types.StringType(), True),
        types.StructField("RatecodeID", types.IntegerType(), True),
        types.StructField("PULocationID", types.IntegerType(), True),
        types.StructField("DOLocationID", types.IntegerType(), True),
        types.StructField("passenger_count", types.IntegerType(), True),
        types.StructField("trip_distance", types.DoubleType(), True),
        types.StructField("fare_amount", types.DoubleType(), True),
        types.StructField("extra", types.DoubleType(), True),
        types.StructField("mta_tax", types.DoubleType(), True),
        types.StructField("tip_amount", types.DoubleType(), True),
        types.StructField("tolls_amount", types.DoubleType(), True),
        types.StructField("ehail_fee", types.DoubleType(), True),
        types.StructField("improvement_surcharge", types.DoubleType(), True),
        types.StructField("total_amount", types.DoubleType(), True),
        types.StructField("payment_type", types.IntegerType(), True),
        types.StructField("trip_type", types.IntegerType(), True),
        types.StructField("congestion_surcharge", types.DoubleType(), True)
    ])

    yellow_schema = types.StructType([
        types.StructField("VendorID", types.IntegerType(), True),
        types.StructField("tpep_pickup_datetime", types.TimestampType(), True),
        types.StructField("tpep_dropoff_datetime", types.TimestampType(), True),
        types.StructField("passenger_count", types.IntegerType(), True),
        types.StructField("trip_distance", types.DoubleType(), True),
        types.StructField("RatecodeID", types.IntegerType(), True),
        types.StructField("store_and_fwd_flag", types.StringType(), True),
        types.StructField("PULocationID", types.IntegerType(), True),
        types.StructField("DOLocationID", types.IntegerType(), True),
        types.StructField("payment_type", types.IntegerType(), True),
        types.StructField("fare_amount", types.DoubleType(), True),
        types.StructField("extra", types.DoubleType(), True),
        types.StructField("mta_tax", types.DoubleType(), True),
        types.StructField("tip_amount", types.DoubleType(), True),
        types.StructField("tolls_amount", types.DoubleType(), True),
        types.StructField("improvement_surcharge", types.DoubleType(), True),
        types.StructField("total_amount", types.DoubleType(), True),
        types.StructField("congestion_surcharge", types.DoubleType(), True)
    ])


    #read csv file as spark dataframe
    green_df = spark.read \
        .option("header", "true") \
        .schema(green_schema)\
        .csv(green_src)



    #read csv file as spark dataframe
    yellow_df = spark.read \
        .option("header", "true") \
        .schema(yellow_schema)\
        .csv(yellow_src)




    #rename columns of green_df
    green_df = green_df\
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime')\
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

    #rename columns of yellow_df
    yellow_df = yellow_df\
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime')\
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')



    common_columns = []

    for col in green_df.columns:
        if col in yellow_df.columns:
            common_columns.append(col)



    #add new column which 'service_type', PySpark lit() function is used to add a constant value to a DataFrame column.
    df_green_select = green_df\
    .select(common_columns)\
    .withColumn('service_type', f.lit('Green'))

    #add new column which 'service_type', PySpark lit() function is used to add a constant value to a DataFrame column.
    df_yellow_select = yellow_df\
    .select(common_columns)\
    .withColumn('service_type', f.lit('Yellow'))


    # union all the two dataframes
    #df_trips_data = df_green_select.unionAll(df_yellow_select)


    green_df.createOrReplaceTempView('green_df_revenue')


    green_df_revenue = spark.sql("""
    select
        extract(HOUR FROM pickup_datetime) as hour,
        PULocationID as zone,
        sum(total_amount) as total_revenue,
        count(1) as count_records
    from 
        green_df_revenue
    group by 1, 2
    order by 3 desc
    """)


    yellow_df.createOrReplaceTempView('yellow_table_revenue')


    yellow_df_revenue = spark.sql("""
    select
        extract(HOUR FROM pickup_datetime) as hour,
        PULocationID as zone,
        sum(total_amount) as total_revenue,
        count(1) as count_records
    from 
        yellow_table_revenue
    group by 1, 2
    order by 3 desc
    """)

    """
    yellow_df_revenue\
    .repartition(20)\
    .write.format("parquet").mode("overwrite").save("trips_report/revenue/yellow")
    """

    yellow_df_tmp = yellow_df_revenue\
    .withColumnRenamed('total_revenue', 'yellow_total_revenue')\
    .withColumnRenamed('count_records', 'yellow_count_records')

    green_df_tmp = green_df_revenue\
    .withColumnRenamed('total_revenue', 'green_total_revenue')\
    .withColumnRenamed('count_records', 'green_count_records')


    # Full Outer join: Returns all rows from both DataFrames, including matching and non-matching rows.
    df_join = yellow_df_tmp.join(green_df_tmp,on=['hour', 'zone'],how="outer") 

    #df_join.write.mode("overwrite").parquet('data/trips_joined_data')


    # read zones file
    zones_df = spark.read\
    .option("header", "true")\
    .csv(zones_src)


    # Inner join: Returns only matched rows between two dataframes
    zones_df_join = df_join.join(zones_df, df_join.zone == zones_df.LocationID , how="inner") 
    zones_df_join.drop('zone', 'LocationID').write.parquet(joined_output, mode='overwrite')

if __name__ == '__main__':
    # Create the parser
    parser = argparse.ArgumentParser()
    # Add an argument:
        #green_path
        #yellow_path
        #zones_path
        #output_path

    parser.add_argument('--green_path', required=True, help="GCS path source where green trips file located")
    parser.add_argument('--yellow_path', required=True, help="GCS path source where yellow trips file located")
    parser.add_argument('--zones_path', required=True, help="GCS path source where zones taxi lookup file located")
    parser.add_argument('--output_path', required=True, help="GCS path source where processed/joine data added ")


    # Parse the argument
    args = parser.parse_args()

    main(args)

"""
to run this script in command line:
1. Change the working directory to the spark directory: cd %SPARK_HOME%
2. To start Spark Master:bin\spark-class org.apache.spark.deploy.master.Master --host localhost
3. Starting up a cluster:bin\spark-class org.apache.spark.deploy.worker.Worker spark://localhost:7077 --host localhost

4. spark-submit --master="spark://localhost:7077" pyspark_sql.py --green_path={your_path} 
--yellow_path={your_path} --zones_path={your_path}
--output_path={your_path}
"""