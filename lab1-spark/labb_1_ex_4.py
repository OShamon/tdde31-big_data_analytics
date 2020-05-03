
from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
lines_temp = temperature_file.map(lambda line: line.split(";"))
lines_pre = precipitation_file.map(lambda line: line.split(";"))
# (key, value) = (year,temperature)
station_temperature = lines_temp.map(lambda x: (x[0],float(x[3])))
station_precipitation = lines_pre.map(lambda x: (x[0], float(x[3])))

#filter

#Get max

max_temperatures = station_temperature.reduceByKey(lambda a,b: max(a, b))
max_precipitation = station_precipitation.reduceByKey(lambda a,b: max(a, b))

filtred_temp = max_temperatures.filter(lambda x: x[1]<=30.0 and x[1] >= 25.0)
filtred_precipitation = max_precipitation.filter(lambda x: x[1] <= 200.0 and x[1] >= 100.0)

joined_tem_pre = filtred_precipitation.join(filtred_temp)
joined_tem_pre = joined_tem_pre.sortBy(ascending = False, keyfunc=lambda k:k[1])


#print(max_temperatures.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
joined_tem_pre.saveAsTextFile("BDA/output")


