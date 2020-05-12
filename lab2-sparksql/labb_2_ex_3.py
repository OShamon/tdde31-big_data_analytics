from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

file = sc.textFile("BDA/input/temperature-readings.csv")

parts = file.map(lambda l: l.split(";"))
#year, month, station, avgMonthlyTemperature ORDER BY avgMonthlyTemperature DESC
tempReadings = parts.map(lambda p: Row(station=p[0], date=p[1], year=p[1].split("-")[0], month=p[1].split("-")[1], time=p[2], value=float(p[3]), quality=p[4] ))
schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

#Filter out irrelevant years
schemaTempReadingsYear = schemaTempReadings.where(schemaTempReadings['year'] >= 1960)
schemaTempReadingsYear = schemaTempReadingsYear.where(schemaTempReadingsYear['year'] <= 2014)

schemaTempReadingsYear = schemaTempReadingsYear.groupBy('year', 'month', 'station').avg('value').orderBy(['avg(value)'], ascending = [1]).show(1000)                                                                                    