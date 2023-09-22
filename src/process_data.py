import csv
import sqlite3
import multiprocessing

lock = multiprocessing.RLock()

# Function to insert data into the Sensors table
def insert_sensor(conn, sensor):
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO Sensors (sensorName)
            VALUES (?)
        ''', (sensor,))
        conn.commit()
    except:
        pass
    lock.release()

# Function to insert data into the Detections table
def insert_detection(conn, detection):
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO Detections (timestamp, sensorId, value)
            VALUES (?, ?, ?)
        ''', detection)
        conn.commit()
    except:
        pass
    lock.release()

# Function to process a single CSV file
def process_csv_file(file_path):
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip the header row
        sensor_id = None

        for row in csv_reader:
            conn = sqlite3.connect('sensor_data.db')
            cursor = conn.cursor()
            sensor_name = row[1]  # Get sensor name from the first row
            # Check if the sensor already exists in the Sensors table
            lock.acquire()
            cursor.execute('SELECT sensorId FROM Sensors WHERE sensorName = ?', (sensor_name,))
            lock.release()
            result = cursor.fetchone()

            if result is None:
                # Sensor doesn't exist, insert it into the Sensors table
                lock.acquire()
                insert_sensor(conn, sensor_name)
                cursor.execute('SELECT sensorId FROM Sensors')
                sensor_id = cursor.lastrowid
            else:
                # Sensor already exists, retrieve its sensorId
                sensor_id = result[0]

            # Check if the detection already exists in the Detections table
            timestamp = row[0]
            value = row[2]
            lock.acquire()
            insert_detection(conn, (timestamp, sensor_id, value))

            # Close the connection
            conn.close()

# Create a connection to the SQLite database
conn = sqlite3.connect('sensor_data.db')

# Create the Sensors and Detections tables if they don't exist
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS Sensors (
        sensorId INTEGER PRIMARY KEY AUTOINCREMENT,
        sensorName TEXT
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS Detections (
        detectionId INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        sensorId INTEGER,
        value REAL,
        FOREIGN KEY (sensorId) REFERENCES Sensors (sensorId)
        unique (timestamp,sensorId,value)
    )
''')
conn.commit()

# Close the connection
conn.close()

# Read CSV files and process them in parallel
directory = './'

# List all CSV files in the directory
csv_files = ['file1.csv', 'file2.csv', 'file3.csv']  # Replace with actual list of CSV files

# Calculate the number of processes to use
max_processes = multiprocessing.cpu_count() - 1

# Create a process pool with the maximum number of processes
pool = multiprocessing.Pool(processes=max_processes)

# Map the CSV files to the process pool for parallel processing
pool.map(process_csv_file, [directory + csv_file for csv_file in csv_files])

# Close the process pool and wait for all processes to finish
pool.close()
pool.join()
