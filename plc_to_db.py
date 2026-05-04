# import common modules
import json
import threading
import queue
import time
import schedule
import pytz
from datetime import datetime
import yaml
import os
import sys

# import mqtt modules
import paho.mqtt.client as mqtt

# import database modules
import psycopg2
import psycopg2.pool

# import siemens plc modules
import snap7
from snap7.type import Areas


"""
Setting basic information
"""

# Loading yaml configuration file
def load_config(config_path: str):
    if not os.path.exists(config_path):
        print(f"Error, there is no configuration file: '{config_path}'")
        sys.exit(1)

    with open(config_path, 'r', encoding='utf-8') as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as exc:
            print(f"Error: {exc}")
            sys.exit(1)


config_path = 'HMI_Control.yml'
config = load_config(config_path)

# Database info
DB_HOST = config['database']['DB_HOST']
DB_PORT = config['database']['DB_PORT']
DB_USER = config['database']['DB_USER']
DB_NAME = config['database']['DB_NAME']
DB_PASSWORD = config['database']['DB_PASSWORD']

# MQTT info
MQTT_BROKER = config['mqtt']['MQTT_BROKER']
MQTT_PORT = config['mqtt']['MQTT_PORT']
MQTT_TOPIC = config['mqtt']['MQTT_TOPIC']

# PLC info
PLC_IP = config['plc']['ip']
PLC_RACK = config['plc']['rack']
PLC_SLOT = config['plc']['slot']
DB_NUMBER = config['plc']['db_number']

# Institution info
INSTITUTION = config.get('institution_abbr', 'Unknown')
# -- Set time_zone --
INSTITUTION_TIMEZONES = {
    "CMU": "America/New_York",
    "IHEP": "Asia/Shanghai",
    "NTU": "Asia/Taipei",
    "TTU": "America/Chicago",
    "TIFR": "Asia/Kolkata",
    "UCSB": "America/Los_Angeles"
}
TIME_ZONE = INSTITUTION_TIMEZONES[INSTITUTION]


# Vaisala dew point sensor scale factor
def act_dew_point(T_plc: float) -> float:
    """
    The scalar factor is needed because of a bug in the PLC programming.

    In the PLC analogue module, they are programmed to measure the dew point
    with a current range of 0 mA to 20 mA; however, the Vaisala dew point
    sensor measures the dew point with a current range of 4 mA to 20 mA.
    This PLC bug results in inaccurate readings, so we need to account for
    this when calculating the true dew point.
    """
    T_real = 1.25 * T_plc - 5
    return T_real


# Sensor id information
sensor_id_list = [
    "RTD-01",
    "RTD-02",
    "RTD-03",
    "RTD-04",
    "RTD-05",
    "RTD-06",
    "RTD-07",
    "RTD-08",
    "DMT-01",
    "DMT-02",
    "Chiller-01",
    "Chiller-T",
    "Chiller-PrevT",
    "System Status"
]

# workers initialize
num_workers = 8
workers = []

# Set up a queue for the database connection
msg_queue = queue.Queue()

# Global PLC client and processing lock
plc_client = None
plc_lock = threading.Lock()

# Global DB pool
db_pool = None

# Store previous sensor input state.
last_seen_data = None
last_seen_signature = None
last_seen_lock = threading.Lock()


# Set up threaded operation
def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()


"""
set up PLC connection
"""

def init_plc():
    global plc_client

    client = snap7.client.Client()
    client.connect(PLC_IP, PLC_RACK, PLC_SLOT)

    plc_client = client
    print("PLC connection established")


"""
MQTT callback functions
"""

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Connected with result code {rc}")
    client.subscribe(MQTT_TOPIC, qos=1)


def on_message(client, userdata, msg):
    try:
        msg_queue.put(msg.payload.decode("utf-8"))
        print(f"Received message: {msg.payload}")
    except Exception as e:
        print(f"Error occurred in on_message: {e}")


def on_disconnect(client, userdata, flags, rc, properties=None):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Disconnected with result code {rc}")


def on_log(client, userdata, level, buf):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - MQTT Log: {buf}")


"""
set up mqtt server connection
"""

# Publish the data to the MQTT broker
def publish_mqtt_batch(data):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)

        payload = json.dumps(data)
        client.publish(MQTT_TOPIC, payload, qos=1)

        print(f"Published payload to MQTT: {payload}")

    except Exception as e:
        print(f"Cannot publish data to MQTT: {e}")

    finally:
        client.disconnect()


"""
setting database
"""

def db_pool_setting():
    global db_pool

    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(
            1,
            20,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME
        )

        if db_pool:
            print("PostgreSQL connection pool created")

    except Exception as e:
        print(f"Cannot create PostgreSQL connection pool: {e}")
        sys.exit(1)

    return db_pool


"""
Duplicate / state-change checking helper
"""

def make_data_signature(data):
    """
    Create a comparable signature from sensor values only.

    `log_timestamp` is excluded because it changes every time,
    even when all sensor readings are physically the same.

    The signature includes:
    - device name
    - value
    - metric

    Float values are rounded to avoid tiny PLC floating-point noise.
    """

    signature = {}

    for sensor_id in sensor_id_list:
        value = data[sensor_id][0]
        metric = data[sensor_id][1]

        if isinstance(value, float):
            value = round(value, 4)

        signature[sensor_id] = [value, metric]

    return signature


def insert_sensor_data_to_db(data, worker_id, reason=""):
    """
    Insert one full sensor payload into the database.

    One payload contains multiple sensors, so this function inserts
    one row per sensor_id.
    """

    conn = None
    cursor = None

    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()

        time_str = data['measured_at']

        # setup the timezone:
        timezone = pytz.timezone(TIME_ZONE)
        naive_time = datetime.fromisoformat(time_str)
        local_time = timezone.localize(naive_time)

        # formate the time to UTC timezone before inserting into database
        utc_time = local_time.astimezone(pytz.utc)

        insert_sql = """
            INSERT INTO mmts_sensors_logging
                (device_name, metric, value, log_timestamp, timestamp_utc)
            VALUES
                (%s, %s, %s, %s, %s)
        """

        for sensor_id in sensor_id_list:
            cursor.execute(
                insert_sql,
                (
                    sensor_id,
                    data[sensor_id][1],
                    data[sensor_id][0],
                    local_time,
                    utc_time
                )
            )

        conn.commit()
        print(f"Worker {worker_id}: inserted sensor data {reason}")

    except Exception as e:
        print(f"Worker {worker_id} database insert error: {e}")

        if conn:
            conn.rollback()

    finally:
        if cursor:
            cursor.close()

        if conn:
            db_pool.putconn(conn)


"""
Database write worker thread, retrieves messages from the queue and writes to PostgreSQL.
"""

def db_worker(worker_id):
    global last_seen_data
    global last_seen_signature

    while True:
        msg_payload = msg_queue.get()

        if msg_payload is None:
            print(f"Worker {worker_id} stopping")
            msg_queue.task_done()
            break

        try:
            data = json.loads(msg_payload)
            current_signature = make_data_signature(data)

            # This list stores the data payloads that should be inserted.
            # We collect them inside the lock, then insert outside the lock.
            data_to_insert_list = []

            with last_seen_lock:
                # first input:
                if last_seen_signature is None:
                    data_to_insert_list.append(
                        (data, "because this is the first input")
                    )

                    last_seen_signature = current_signature
                    last_seen_data = data

                    print(f"Worker {worker_id}: first input, will insert current data")

                # same input as previous one, no state change:
                elif current_signature == last_seen_signature:
                    last_seen_data = data

                    print(
                        f"Worker {worker_id}: same input as previous one, "
                        f"skip insert but update latest cached data"
                    )

                # input changes:
                else:
                    data_to_insert_list.append(
                        (last_seen_data)
                    )

                    data_to_insert_list.append(
                        (data)
                    )

                    last_seen_signature = current_signature
                    last_seen_data = data

                    print(
                        f"Worker {worker_id}: input changed, "
                        f"will insert previous latest data and current data"
                    )

            # Insert outside the lock so database insert does not block
            # other workers from checking incoming data.
            for insert_data, reason in data_to_insert_list:
                insert_sensor_data_to_db(insert_data, worker_id, reason)

        except Exception as e:
            print(f"Worker {worker_id} error: {e}")

        finally:
            msg_queue.task_done()


def workers_setting():
    for i in range(num_workers):
        t = threading.Thread(target=db_worker, args=(i + 1,), daemon=True)
        t.start()
        workers.append(t)


"""
PLC work functions
"""

# Get the data from the PLC
def read_sensor_real(offset):
    with plc_lock:
        data = plc_client.db_read(DB_NUMBER, offset, 4)

    return snap7.util.get_real(data, 0)


def read_sensor_bool(byte_offset, bit_index):
    with plc_lock:
        data = plc_client.db_read(DB_NUMBER, byte_offset, 1)

    return snap7.util.get_bool(data, 0, bit_index)


def read_m_bool(byte_offset, bit_index):
    with plc_lock:
        data = plc_client.read_area(Areas.MK, 0, byte_offset, 1)

    return snap7.util.get_bool(data, 0, bit_index)


def system_status() -> list:
    """
    Reading the current operational status of the chiller.

    The function will send out an integer to represent the operation status:

    0. Door open / system check needed
    1. Standby
    2. Countdown - Warming Stage
    3. Warming Up
    4. Countdown - Cooling Stage
    5. Cooling Down
    """

    status_code = 0

    status_dict = {
        0: 'The door is open. Please check the system.',
        1: 'Standby',
        2: 'Countdown-Warming Stage',
        3: 'Warming up',
        4: 'Countdown-Cooling Stage',
        5: 'Cooling down'
    }

    idle_running = read_sensor_bool(540, 0)
    is_go_cold = read_m_bool(100, 4)
    is_go_warm = read_m_bool(100, 3)
    is_door_lock = read_m_bool(60, 0)

    if not is_door_lock:
        status_code = 0
        return [status_code, status_dict[status_code]]

    if idle_running:
        if is_go_warm:
            status_code = 2
        elif is_go_cold:
            status_code = 4
        else:
            status_code = 1
    else:
        if is_go_warm and not is_go_cold:
            status_code = 3
        elif is_go_cold and not is_go_warm:
            status_code = 5
        else:
            status_code = 1

    return [status_code, status_dict[status_code]]


def schedule_job():
    try:
        # temperature
        rtd01 = read_sensor_real(6)
        rtd02 = read_sensor_real(44)
        rtd03 = read_sensor_real(82)
        rtd04 = read_sensor_real(120)
        rtd05 = read_sensor_real(158)
        rtd06 = read_sensor_real(196)
        rtd07 = read_sensor_real(234)
        rtd08 = read_sensor_real(272)

        # dew point
        dmt01 = act_dew_point(read_sensor_real(314))
        dmt02 = act_dew_point(read_sensor_real(356))

        # chiller temp
        chiller01 = read_sensor_real(410)
        chiller02 = read_sensor_real(418)
        chiller03 = read_sensor_real(422)

        # system status
        sys_status = system_status()[0]

        # Combine the data into a single payload
        data = {
            "RTD-01": [rtd01, 'temperature_C'],
            "RTD-02": [rtd02, 'temperature_C'],
            "RTD-03": [rtd03, 'temperature_C'],
            "RTD-04": [rtd04, 'temperature_C'],
            "RTD-05": [rtd05, 'temperature_C'],
            "RTD-06": [rtd06, 'temperature_C'],
            "RTD-07": [rtd07, 'temperature_C'],
            "RTD-08": [rtd08, 'temperature_C'],
            "DMT-01": [dmt01, 'dewpoint_C'],
            "DMT-02": [dmt02, 'dewpoint_C'],
            "Chiller-01": [chiller01, 'temperature_C'],
            "Chiller-T": [chiller02, 'temperature_C'],
            "Chiller-PrevT": [chiller03, 'temperature_C'],
            "System Status": [sys_status, 'system_C'],
            "measured_at": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        publish_mqtt_batch(data)

    except Exception as e:
        print(f"Error reading PLC data: {e}")


def main():
    # Set up DB pool and workers
    db_pool_setting()
    workers_setting()

    # Schedule the job with 20 seconds interval
    schedule.every().minute.at(":00").do(run_threaded, schedule_job)
    schedule.every().minute.at(":20").do(run_threaded, schedule_job)
    schedule.every().minute.at(":40").do(run_threaded, schedule_job)

    # Set up the MQTT client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.on_log = on_log

    # Connect to the MQTT broker
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    # Establish connection to the PLC
    init_plc()

    try:
        while True:
            schedule.run_pending()
            time.sleep(10)
            print(f"Queue size: {msg_queue.qsize()}")

    except KeyboardInterrupt:
        print("Exiting")

    except Exception as e:
        print(f"Error occurred: {e}")

    finally:
        client.loop_stop()

        for _ in range(num_workers):
            msg_queue.put(None)

        for worker in workers:
            worker.join()

        if db_pool:
            db_pool.closeall()
            print("PostgreSQL connection pool closed")


if __name__ == "__main__":
    main()