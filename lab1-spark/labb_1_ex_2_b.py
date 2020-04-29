from pyspark import SparkContext


sc = SparkContext(appName = "exercise test")
temp_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temp_file.map(lambda line: line.split(";"))
year_temp = lines.map(lambda x: ((x[0], x[1][0:4], x[1][5:7]), float(x[3])))
year_temp = year_temp.filter(lambda x: int(x[0][1]) >= 1950 and int(x[0][1]) <=$
over_10 = year_temp.filter(lambda x: int(x[1]) >= 10)
over_10_count = over_10.map(lambda x: (x[0], 1))
singel_station = over_10_count.reduceByKey(lambda v1, v2: v1)
without_station = singel_station.map(lambda x: (x[0][1:3], 1))
count = without_station.reduceByKey(lambda v1, v2: v1 + v2)
max_tempsorted = count.sortBy(ascending  = False, keyfunc = lambda k:[1])
max_tempsorted.saveAsTextFile("BDA/output")



