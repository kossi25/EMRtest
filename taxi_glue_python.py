import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

#from pyspark import SparkContext
#from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark import SQLContext
from itertools import islice
#from pyspark.sql.functions import col


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "bbb-glue-crawler-taxi-db", table_name = "taxidata_csv", transformation_ctx = "datasource0")

applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("vendorid", "long", "vendorid", "long"), ("lpep_pickup_datetime", "string", "lpep_pickup_datetime", "string"), ("lpep_dropoff_datetime", "string", "lpep_dropoff_datetime", "string"), ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"), ("ratecodeid", "long", "ratecodeid", "long"), ("pulocationid", "long", "pulocationid", "long"), ("dolocationid", "long", "dolocationid", "long"), ("passenger_count", "long", "passenger_count", "long"), ("trip_distance", "double", "trip_distance", "double"), ("fare_amount", "double", "fare_amount", "double"), ("extra", "double", "extra", "double"), ("mta_tax", "double", "mta_tax", "double"), ("tip_amount", "double", "tip_amount", "double"), ("tolls_amount", "double", "tolls_amount", "double"), ("ehail_fee", "string", "ehail_fee", "string"), ("improvement_surcharge", "double", "improvement_surcharge", "double"), ("total_amount", "double", "total_amount", "double"), ("payment_type", "long", "payment_type", "long"), ("trip_type", "long", "trip_type", "long")], transformation_ctx = "applymapping1")

selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["vendorid", "lpep_pickup_datetime", "lpep_dropoff_datetime", "store_and_fwd_flag", "ratecodeid", "pulocationid", "dolocationid", "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "ehail_fee", "improvement_surcharge", "total_amount", "payment_type", "trip_type"], transformation_ctx = "selectfields2")

resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "bbb-glue-crawler-taxi-db", table_name = "taxidata_csv", transformation_ctx = "resolvechoice3")


resolvechoice3.toDF().createOrReplaceTempView("taxi")


resolvechoice3 = spark.sql("SELECT PULocationID, DOLocationID, passenger_count, trip_distance, RatecodeID, \
                    total_amount, payment_type, trip_type, tip_amount, fare_amount, \
                    ROUND(CAST(tip_amount/fare_amount AS DOUBLE), 4) as tip_percent, \
                    CAST(from_unixtime(unix_timestamp(lpep_pickup_datetime, 'MM/dd/yyyy hh:mm:ss aa'), 'yyyy') AS INT) as pickup_year, \
                    CAST(from_unixtime(unix_timestamp(lpep_pickup_datetime, 'MM/dd/yyyy hh:mm:ss aa'), 'MM') AS INT) as pickup_month,\
                    CAST(from_unixtime(unix_timestamp(lpep_pickup_datetime, 'MM/dd/yyyy hh:mm:ss aa'), 'dd') AS INT) as pickup_day, \
                    CAST(from_unixtime(unix_timestamp(lpep_pickup_datetime, 'MM/dd/yyyy hh:mm:ss aa'), 'hh') AS INT) as pickup_hour, \
                    CAST(from_unixtime(unix_timestamp(lpep_pickup_datetime, 'MM/dd/yyyy hh:mm:ss aa'), 'mm') AS INT) as pickup_minute, \
                    CAST(from_unixtime(unix_timestamp(lpep_dropoff_datetime, 'MM/dd/yyyy hh:mm:ss aa'), 'yyyy') AS INT) as dropoff_year, \
                    CAST(from_unixtime(unix_timestamp(lpep_dropoff_datetime, 'MM/dd/yyyy hh:mm:ss aa'), 'MM') AS INT) as dropoff_month,\
                    CAST(from_unixtime(unix_timestamp(lpep_dropoff_datetime, 'MM/dd/yyyy hh:mm:ss aa'), 'dd') AS INT) as dropoff_day, \
                    CAST(from_unixtime(unix_timestamp(lpep_dropoff_datetime, 'MM/dd/yyyy hh:mm:ss aa'), 'hh') AS INT) as dropoff_hour, \
                    CAST(from_unixtime(unix_timestamp(lpep_dropoff_datetime, 'MM/dd/yyyy hh:mm:ss aa'), 'mm') AS INT) as dropoff_minute, \
                    ROUND(CAST((unix_timestamp(lpep_dropoff_datetime, 'MM/dd/yyyy hh:mm:ss aa') - unix_timestamp(lpep_pickup_datetime, 'MM/dd/yyyy hh:mm:ss aa'))/360 AS DOUBLE), 4) as tripdurr \
                    FROM taxi WHERE fare_amount > 2.50")
                    
resolvechoice3 = DynamicFrame.fromDF(resolvechoice3, glueContext, "nested")

datasink4 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice3, database = "bbb-glue-crawler-taxi-db", table_name = "taxidata_csv", transformation_ctx = "datasink4")
output_dir = "s3://aws-emr-resources-507786327009-us-east-1/medicare_parquet"
glueContext.write_dynamic_frame.from_options(frame = resolvechoice3, connection_type = "s3", connection_options = {"path": output_dir}, format = "parquet")
job.commit()