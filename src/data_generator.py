import datetime
import csv
import numpy as np

sensor_name_list = ["a","b","c","d","e"]

def generate_sensor_data():
    timestamp = datetime.datetime.now().astimezone().isoformat()
    sensorName = np.random.choice(sensor_name_list)
    value = np.random.rand()
    return [timestamp, sensorName, value]

data_header = ["timestamp", "sensorName", "value"]

for i in range(1,4):
    with open(f"file{i}.csv","w+") as file:
        writer = csv.writer(file)
        writer.writerow(data_header)

        for i in range(1000):
            writer.writerow(generate_sensor_data())


