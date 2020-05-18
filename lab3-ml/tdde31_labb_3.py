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
def gaussian_kernel_dist(station_id, station_list, lon, lat, h_distance):
    for station in station_list:
        if int(station[0]) == int(station_id):
            return exp(-((haversine(float(station[3]), float(station[4]), lon, lat)/h_distance)**2))
    return None

def gaussian_kernel_time(time_1, time_2, h_time):
    h1 = int(time_1.split(":")[0])
    h2 = int(time_2.split(":")[0])
    return exp(-((h1-h2)/h_time)**2)

def gaussian_kernel_date(date_1, date_2, h_date):
    d1 = int(date_1.split("-")[1])*31 + int(date_1.split("-")[2])
    d2 = int(date_2.split("-")[1])*31 + int(date_2.split("-")[2])
    
    return exp(((d1 - d2)/h_date)**2)

temperature_file = sc.textFile("BDA/input/temperature-readings-small.csv")
stations_file = sc.textFile("BDA/input/stations.csv")
lines_temp = temperature_file.map(lambda line: line.split(";"))
lines_station = stations_file.map(lambda line: line.split(";"))

stations = lines_station.map(lambda x: (x[0], (x[3], x[4])))
stations_b = sc.broadcast(stations.collect())

list_of_predictions = []                                     
                                     
for time in ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00",
"12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:
    temprature_readings = lines_temp.map(lambda x: ((x[0], x[1][0:4], x[1][5:7], x[1][8:10]), gaussian_kernel_dist(x[0], stations_b.value, a, b, h_distance), gaussian_kernel_date(x[1], date), gaussian_kernel_time(x[2], time), float(x[3])))
    temprature_readings = temprature_readings.map(lambda x: (x[0], ((x[1][0] + x[1][1] + x[1][2])*x[1][3], (x[1][0] + x[1][1] + x[1][2])))).cache()
    
    reduced =  temprature_readings.reduce(lambda x: (x[1][0] + x[1][0], x[1][1] + x[1][1]))
    
    list_of_predictions.append(reduced.reduce(lambda x: x[0]/x[1]))
    

print(list_of_predictions)
