
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

file = sc.textFile("BDA/input/temperature-readings.csv")

parts = file.map(lambda l: l.split(";"))

tempReadings = parts.map(lambda p: Row(station=p[0], date=p[1], year=p[1].split("-")[0], month=p[1].split("-")[1], time=p[2], value=float(p[3]), quality=p[4] ))

schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

schemaTempReadingsMax = schemaTempReadings.groupBy('year', 'station', 'month').agg(F.max('value').alias('value'))

schemaTempReadings10 = schemaTempReadingsMax.where(schemaTempReadingsMax['value'] >= 10)

schemaTempReadingsCount = schemaTempReadings10.groupBy('year', 'month').count().orderBy(['count'], ascending = [1]).show(1000)




