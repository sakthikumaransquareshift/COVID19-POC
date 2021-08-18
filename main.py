# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from datetime import timedelta

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *  # List of data types available.
from pyspark.sql.window import Window
from influxdb_client import *
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from awsglue.utils import getResolvedOptions
import job_initialization as job_init

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INFLUX_URL', 'INFLUX_TOKEN', 'INFLUX_ORG'])
sc=SparkContext()
gc=GlueContext(sc)
ss= gc.spark_session
job = Job(gc)
job.init(args['JOB_NAME'], args)
sparkContext = job_init.sparkContext
glueContext = job_init.glueContext
sparkSession = job_init.sparkSession
sqlContext = job_init.sqlContext

mysqlconnection = None
influxsqlconnection = None

def connectToMySQLDB(serverUrl, username, password):
    mysqlconnection = ss.read.format("jdbc")
    options = {
        "url": "jdbc;//{0}".format(serverUrl),
        "driver": "com.mysql.jdbc.Driver",
        "user": username,
        "password": password
    }
    mysqlconnection.option("url", options["url"]).option("driver", options["driver"])\
         .option("user", options["user"])\
         .option("password", options["password"])\
         .option("hashfield", '5').option("numPartitions", 8)

    return mysqlconnection

def connectToInflux(url, token, org):
    options = {
        "url": url,
        "token": token,
        "org": org
    }
    client = InfluxDBClient(options)
    query_api = client.query_api()

    return query_api

 #"https://us-east-1-1.aws.cloud2.influxdata.com/api/v2/","c8NwqFMz9-EdFAzXd-jnCHXG6613RP3Z8PY8J2d5224q8HXkeOZzguw8A5aqaTXvPs8eBJVaAlrKhgmsrDRbZQ==",
 #"sakthikumaran@squareshift.co"
def queryAllInfoInflux(query):
    influxsqlconnection = connectToInflux(args[0],
                                          args[1],
                                          args[2])

    result = influxsqlconnection.query(query=query)

    return result;

def compareCasesForEachCountryWith(y):
    redval = -1
    diff = 0
    k = list(y)
    k.sort(key=lambda x: x.Last_Update)
    for i in k:
        if i.Totalcases != 0:
            if redval != -1:
                diff = redval - i.Totalcases
                if diff <= 0:
                    break
            redval = i.Totalcases

    totalcases = 0
    country = None

    if diff > 0:
        for i in k:
            country = i.Country_Region
            totalcases += i.Totalcases
        yield [country, totalcases]
    else:
        yield ['NA', 0.0]


def findNImprovedCountriesWithHighestProvinceforEach(nDays, n, m, datetime=None):
    nDays += 1
    datenow = datetime.now()
    nDaysAgoDate = (datenow + timedelta(days=-nDays))

    query = ' from (bucket:"my-bucket") \
          |> range(start: -10m)\
          |> select(fn: (r) = > r.Country_Region, r.Province_State,r.Last_Update, r.Recovered,r.Confirmed,r.Deaths)'

    result = queryAllInfoInflux(query);
    schema = StructType[
        StructField["Country_Region", StringType(), True],
        StructField["Province_State", StringType(), True],
        StructField["Last_Update", StringType(), True],
        StructField["Recovered", IntegerType(), True],
        StructField["Confirmed", IntegerType(), True],
        StructField["Deaths", IntegerType(), True]
    ]

    df = gc.create_data_frame(schema, result["results"]["values"])

    df1 = df.filter(col("Last_Update") > nDaysAgoDate) \
        .select("Country_Region", coalesce(col("Province_State"), lit("NA")).alias("Province_State"),
                "Last_Update", (coalesce(col("Confirmed"), lit(0.0)) + coalesce(col("Deaths"),
                lit(0.0)) + coalesce(col("Recovered"), lit(0.0))).alias("Totalcases"))

    # find total cases for each province
    df2 = df1.groupBy("Country_Region", "Last_Update") \
        .sum('Totalcases') \
        .withColumnRenamed("sum(Totalcases)", "Totalcases")

    # partitionBy Country - expensive?
    windowSpec = Window.partitionBy("Country_Region").orderBy("Country_Region")
    df3 = df2.withColumn("row_number", row_number().over(windowSpec))

    # find countries with decreasing cases by comparing within partition
    rdd3 = df3.rdd.mapPartitions(compareCasesForEachCountryWith)

    # convert partitioned RDD to DF
    schema = StructType([
        StructField("Country_Region", StringType(), True),
        StructField("Country_Totalcases", DoubleType(), True)
    ])
    df5 = rdd3.toDF(schema).filter(col("Country_Region") != 'NA') \
        .orderBy(col('Country_Totalcases')) \
        .select("Country_Region") \
        .limit(n)

    # project cases in each province of the top n countries
    df7 = df1.join(df5, 'Country_Region', 'inner') \
        .select("Country_Region", 'Province_State', 'Totalcases')\
        .groupBy("Country_Region", "Province_State") \
        .sum('Totalcases') \
        .withColumnRenamed("sum(Totalcases)", "Totalcases") \
        .orderBy(col('Totalcases')) \

    # select top m province for each countries-expensive?
    windowProvince = Window.partitionBy('Country_Region').orderBy(col("Totalcases").desc())
    df8 = df7.withColumn("row", row_number().over(windowProvince)) \
        .filter(col("row") <= m) \
        .select("Country_Region", 'Province_State', 'Totalcases')
    df8.coalesce(1).write.mode("overwrite").format("parquet").save("s3://mysamplebucketsakthi/output/output.parquet")


findNImprovedCountriesWithHighestProvinceforEach(18, 10, 3)
job.commit()






