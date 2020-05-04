from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
ost_stations_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
lines_ost = ost_stations_file.map(lambda line: line.split(";"))
lines_pre = precipitation_file.map(lambda line: line.split(";"))
# (key, value) = (year,temperature)
stations = lines_ost.map(lambda x: (int(x[0])))
station_precipitation = lines_pre.map(lambda x: ((x[0], x[1][0:4], x[1][5:7]), float(x[3])))
stations_list = stations.collect()

broadcasted_list = sc.broadcast(stations_list)

#filter
year_filter = station_precipitation.filter(lambda x: int(x[0][1]) >= 1993 and int(x[0][1]) <= 2016)
filtred_precipitation = year_filter.filter(lambda x: int(x[0][0]) in broadcasted_list.value)
#filtred_precipitation.saveAsTextFile("BDA/output")
#Get max
filtred_precipitaion = filtred_precipitation.reduceByKey(lambda a, b: a + b)


count_pre = filtred_precipitaion.map(lambda x: ((x[0][1], x[0][2]), 1))
#put it together to get number of readings for same year, month, station number
count_pre = count_pre.reduceByKey(lambda x, y: x+y)

precipitations_redding_without_station = filtred_precipitaion.map(lambda x: ((x[0][1], x[0][2]), x[1]))
#add everything together, will get total te

total_pre = precipitations_redding_without_station.reduceByKey(lambda x, y: x + y)
#divide total temp with the number of readings


total_pre_and_count = total_pre.join(count_pre)

average_pre = total_pre_and_count.mapValues(lambda x: x[0]/x[1])


# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
average_pre.saveAsTextFile("BDA/output")