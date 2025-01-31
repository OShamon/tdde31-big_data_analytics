
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

stations_Ostergotland = sc.textFile("BDA/input/stations-Ostergotland.csv")
stations_Ostergotland = stations_Ostergotland.map(lambda l: l.split(";"))
stations_Ostergotland = stations_Ostergotland.map(lambda x: Row(station=x[0]))
stations_Ostergotland = sqlContext.createDataFrame(stations_Ostergotland)
stations_Ostergotland.registerTempTable("stations_Ostergotland")



precipitation_readings = sc.textFile("BDA/input/precipitation-readings.csv")
precipitation_readings = precipitation_readings.map(lambda l: l.split(";"))
precipitation_readings = precipitation_readings.map(lambda x: Row(station=x[0], year=x[1][0:4], month=x[1][5:7], value=float(x[3])))
precipitation_readings = sqlContext.createDataFrame(precipitation_readings)
precipitation_readings.registerTempTable("precipitation_readings")

precipitation_readings_OST = precipitation_readings.join(stations_Ostergotland, ['station'], 'inner').select('year', 'month', 'value', 'station')
precipitation_readings_OST = precipitation_readings_OST.where(precipitation_readings_OST['year'] >= 1993)
precipitation_readings_OST = precipitation_readings_OST.where(precipitation_readings_OST['year'] <= 2016)


precipitation_readings_OST = precipitation_readings_OST.groupBy('year', 'month', 'station').sum('value')

precipitation_readings_OST = precipitation_readings_OST.groupBy('year', 'month').avg('sum(value)').orderBy(['year', 'month'], ascending = [1,1]).show(1000)
