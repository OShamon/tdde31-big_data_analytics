import numpy as np
from math import radians, cos, sin, asin, sqrt, exp
import datetime

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
date = "1990-07-04" # Up to you
#stations = sc.textFile("data/stations.csv") 
#temps = sc.textFile("data/temps.csv")
# Your code here


def gaussian_kernel_dist(lon_lat, lon, lat, h_distance):
    return exp(-((haversine(lon_lat[0], lon_lat[1], lon, lat)/h_distance)**2))


def gaussian_kernel_time(time_1, time_2, h_time):
    h1 = int(time_1.split(":")[0])
    h2 = int(time_2.split(":")[0])
    diff = min(abs(h1 - h2), min(h1, h2) + 24 - max(h1, h2))
    return exp(-(((diff)/h_time)**2))


def gaussian_kernel_date(date_1, date_2, h_date):
    #d1 = int(date_1.split("-")[1])*31 + int(date_1.split("-")[2])
    #d2 = int(date_2.split("-")[1])*31 + int(date_2.split("-")[2])
    return exp(-((((date_1 - date_2).days)/h_date)**2))


def get_lon_lat(station_list, station_id):
    for station in station_list:
        if int(station[0]) == int(station_id):
            return station[1]


gaussian_dist = np.zeros((1000, 1000))
dist = np.zeros((1000, 1000))
for i in range(1000):
    for j in range(1000):
        gaussian_dist[i, j] = gaussian_kernel_dist((a + i/500 - 1, b + j/500 - 1), a, b, h_distance)
        dist[i, j] = haversine(a + i/500 - 1, b + j/500 - 1, a, b)
print(gaussian_dist[:, 500])
print(dist[:, 500])


time_gaussian = np.zeros((12, 12))
j_count = 0
i_count = 0
for i in ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00",
"12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00", "02:00:00"]:
    j_count = 0
    for j in ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00",
     "12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00", "02:00:00"]:
      time_gaussian[j_count, i_count] = gaussian_kernel_time(i, j, h_time)
      j_count += 1
    i_count += 1

print(time_gaussian[1, :])

date_gaussian = np.zeros(365)
date = datetime.datetime.strptime(date, "%Y-%M-%d")
date_date = date
print(date_date)

for i in range(365):
    date_date += datetime.timedelta(days=1)
    if date_date.year > date.year:
        date_date -= datetime.timedelta(days=365)
    date_gaussian[i] = gaussian_kernel_date(date, date_date, h_date)

print(date_gaussian)