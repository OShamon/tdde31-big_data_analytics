from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp 
from datetime import datetime
from pyspark import SparkContext
sc = SparkContext(appName="lab_kernel")
def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2]) # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2 
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km
h_distance = 50# Up to you
h_date = 7# Up to you
h_time = 2# Up to you
a = 58.4274 # Up to you
b = 14.826 # Up to you
date = "2013-07-04" # Up to you
stations = sc.textFile("data/stations.csv") 
temps = sc.textFile("data/temps.csv")
# Your code here
def gaussian_kernel_dist(lon_lat, lon, lat, h_distance):
    return exp(-((haversine(lon_lat[0], lon_lat[1], lon, lat)/h_distance)**2))

def gaussian_kernel_time(time_1, time_2, h_time):
    h1 = int(time_1[0:2])
    h2 = int(time_2[0:2])
    diff = min(abs(h1 - h2), min(h1, h2) + 24 - max(h1, h2))
    return exp(-((diff)/h_time)**2)

def gaussian_kernel_date(date_1, date_2, h_date):
    time = (datetime(int(date_1[0:3]), int(date_1[5:6]), int(date_1[8:9])) - datetime(int(date_2[0:3]), int(date_2[5:6]), int(date_2[8:9]))).days
    return exp(-((time)/h_date)**2)

    return exp(-((d1 - d2)/h_date)**2)

def get_lon_lat(station_list, station_id):
    for station in station_list:
        if int(station[0]) == int(station_id):
            return float(station[1][0]), float(station[1][1])


temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
stations_file = sc.textFile("BDA/input/stations.csv")
lines_temp = temperature_file.map(lambda line: line.split(";")).sample(False, 0.1)
lines_station = stations_file.map(lambda line: line.split(";"))

stations = lines_station.map(lambda x: (x[0], (x[3], x[4])))
stations_b = sc.broadcast(stations.collect())
temperature_readings = lines_temp.map(lambda x: (x[0], x[1], x[2], get_lon_lat(stations_b.value, x[0]), float(x[3])))
temperature_readings_lon_lat = temperature_readings.filter(lambda x: datetime(int(x[1][0:4]), int(x[1][5:7]), int(x[1][8:10])) <= datetime(int(date[0:4]), int(date[5:7]), int(date[8:10])))
temperature_readings_lon_lat.cache()

list_of_predictions = []
for time in ["24:00:00", "22:00:00"]: # , "20:00:00", "18:00:00", "16:00:00", "14:00:00", "12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:
    temprature_readings = temperature_readings_lon_lat.map(lambda x: (x[0], gaussian_kernel_dist(x[3], a, b, h_distance), gaussian_kernel_date(x[1], date, h_date), gaussian_kernel_time(x[2], time, h_time), float(x[4])))
    temp_readings = temprature_readings.map(lambda x: (1 , (x[1]*x[4] + x[2]*x[4] + x[3]*x[4], x[1] + x[2] + x[3])))
    #temp_readings.saveAsTextFile("BDA/output")
    reduced =  temp_readings.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    #serializzed.saveAsTextFile("BDA/output")
    list_of_predictions.append(reduced.collect())

#print(list_of_predictions)
list_of = []
for i in list_of_predictions:
    print(i)
    list_of.append(i[0][1][0]/i[0][1][1])
list_of_sc = sc.parallelize(list_of)

list_of_sc.saveAsTextFile("BDA/output")
