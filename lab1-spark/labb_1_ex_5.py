from pyspark import SparkContext
sc = SparkContext(appName = "lab1-part5")

precipitation_reading = sc.textFile("BDA/input/precipitation-readings.csv")
stations_ostergotland = sc.tectFile("BDA/input/stations-Ostergotland.csv")

precipitation_reading = precipitation_reading.map(lambda line: line.split(";"))
stations_ostergotland = stations_ostergotland.map(lambda line: line.split(";"))