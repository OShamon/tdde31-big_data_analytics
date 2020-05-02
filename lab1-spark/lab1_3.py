from pyspark import SparkContext
sc = SparkContext(appName = "lab1-part3")
temperature_file = sc.textFile("")
lines = temperature_file.map( lambda line: line.split(";"))
#year, month, station number, avreage monthly temperature
year_temperature = lines.map(lambda x: ( x[1][0:4], x[1][5:7], x[0], float(x[3]) ))
year_temperature = year_temperature.filter(lambda x: int(x[0]) >= 1960 and int(x[0]) <= 2014)

#count avreage temp for each set of the same year, month, stationnumber
count_temp = year_temperature.map(lambda x: (x[0], x[1], [2], 1))
#put it together to get number of readings for same year, month, station number
count_temp = count_temp.reduceByKey(lambda x, y: x+y)

#add everything together, will get total temp
total_temp = year_temperature.reduceByKey()
#divide total temp with the number of readings




total_temp.saveAsTextFile("lab1_part3_result")