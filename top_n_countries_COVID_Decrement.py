from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from datetime import datetime
from datetime import timedelta
from pyspark import SparkFiles
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

_githuburl = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master' \
             '/csse_covid_19_data/csse_covid_19_daily_reports'

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

import findspark
findspark.init()


# Not used now
def compareCasesForEachCountryWithProvince(y):
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

    dictval = {}
    country = None

    if diff > 0:
        for i in k:
            if i.Province_State in dictval:
                dictval[i.Province_State] += i.Totalcases
            else:
                dictval[i.Province_State] = i.Totalcases
            country = i.Country_Region
        for k in dictval.keys():
            yield [country, k, dictval[k]]
    else:
        yield ['NA', 'NA', 0.0]


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


def findNImprovedCountriesWithHighestProvinceforEach(nDays, n, m):
    nDays += 1
    datenow = datetime.now()
    nDaysAgoDate = (datenow + timedelta(days=-nDays))

    for n in range(2, nDays):
        loopDaysAgoDate = (datenow + timedelta(days=-n))
        formateddate = loopDaysAgoDate.strftime("%m-%d-%Y")
        sc.addFile(f"{_githuburl}/{formateddate}.csv")

    # read the csv files into DF
    rdd = sc.textFile("file:///" + SparkFiles.get("*.csv"))
    df = spark.read.options(header='True', inferSchema='True', mode='DROPMALFORMED') \
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
        .csv(rdd)

    # filter for last 14 days records and dispaly TotalCases fo Province/State of country
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
    df5 = rdd3.toDF(["Country_Region", "Country_Totalcases"])

    # find n countries with maximum cases- expensive?
    df6 = df5.filter(col("Country_Region") != 'NA') \
        .orderBy(col('Country_Totalcases')) \
        .select("Country_Region") \
        .limit(n)

    # project cases in each province of the top n countries
    df7 = df1.join(df6, 'Country_Region', 'inner') \
        .select("Country_Region", 'Province_State', 'Totalcases')

    # calculate total cases per province
    df8 = df7.groupBy("Country_Region", "Province_State") \
        .sum('Totalcases') \
        .withColumnRenamed("sum(Totalcases)", "Totalcases") \
        .orderBy(col('Totalcases')) \

    # select top m province for each countries-expensive?
    windowProvince = Window.partitionBy('Country_Region').orderBy(col("Totalcases").desc())
    df8.withColumn("row", row_number().over(windowProvince)) \
        .filter(col("row") <= m) \
        .select("Country_Region", 'Province_State', 'Totalcases') \
        .show()


findNImprovedCountriesWithHighestProvinceforEach(18, 10, 3)
