from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

fileTemp = sc.textFile("BDA/input/temperature-readings.csv")
filePre = sc.textFile("BDA/input/precipitation-readings.csv")

partsTemp = fileTemp.map(lambda l: l.split(";"))
partsPre = filePre.map(lambda l: l.split(";"))

tempReadings = partsTemp.map(lambda p: Row(station=p[0], date=p[1], year=p[1].split("-")[0], time=p[2], tempValue=float(p[3]), quality=p[4] ))
preReadings = partsPre.map(lambda p: Row(station=p[0], date=p[1], year=p[1].split("-")[0], time=p[2], preValue=float(p[3]), quality=p[4] ))
schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaPreReadings = sqlContext.createDataFrame(preReadings)
schemaTempReadings.registerTempTable("tempReadings")
schemaPreReadings.registerTempTable("preReadings")

schemaPreReadingsDay = schemaPreReadings.groupBy('station', 'date').agg(F.sum('preValue').alias('DailyPre'))
schemaPreReadingsMax = schemaPreReadingsDay.groupBy('station').agg(F.max('DailyPre').alias('MaxPre'))

schemaTempReadingsMax = schemaTempReadings.groupBy('station').agg(F.max('tempValue').alias('MaxTemp'))

totalTempPre = schemaPreReadingsMax.join(schemaTempReadingsMax, ['station'], 'inner').orderBy(['MaxPre'], ascending = [0])

filtredTempPre = totalTempPre.filter((totalTempPre["MaxPre"] >= 100) & (totalTempPre["MaxPre"] <= 200) & (totalTempPre["MaxTemp"] >= 25) & (totalTempPre["MaxTemp"] <= 30)).show()   



