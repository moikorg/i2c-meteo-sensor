import time
import sys
import os
import board
import busio
import time
import adafruit_bme280
import mysql.connector as mariadb
from datetime import datetime
import json
import paho.mqtt.client as mqtt
import configparser
import argparse
import schedule
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS



# some constants
sensor_id = 2
temp_calibration = -0.0


def configSectionMap(config, section):
    dict1 = {}
    options = config.options(section)
    for option in options:
        try:
            dict1[option] = config.get(section, option)
            if dict1[option] == -1:
                print("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1


def parseTheArgs() -> object:
    parser = argparse.ArgumentParser(description='Reads a value from the BME280 sensor and writes it to MQTT and DB')
    parser.add_argument('-f', help='path and filename of the config file, default is ./config.rc',
                        default='config.rc')
    parser.add_argument('-d', help='write the data also to MariaDB/MySQL DB', action='store_true', dest='db_write')

    args = parser.parse_args()
    return args


def connectSensorBME280():
    # Create library object using our Bus I2C port
    sensor_address = 0x76
    i2c = busio.I2C(board.SCL, board.SDA)
    bme280 = adafruit_bme280.Adafruit_BME280_I2C(i2c, sensor_address)
    # change this to match the location's pressure (hPa) at sea level
    #bme280.sea_level_pressure = 1013.25
    return bme280


def getSensorData(sensor_bme280, mqtt_client, db_write, cursor):
    epoch = int(time.time())
    dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    print("Epoch Time: ", epoch)
    print("Datetime: ", dt)

    temp = round(sensor_bme280.temperature + temp_calibration, 2)
    hum = round(sensor_bme280.humidity, 1)
    press = round(sensor_bme280.pressure, 1)
    val = (temp, hum, press, dt, epoch, sensor_id)

    # {"ts":"1544425412","temp":"3.00","hum":"95.92","press":"0"}
    json_data = json.dumps({"ts": epoch,
                            "temp": temp,
                            "hum": hum,
                            "press": press})

    print(str(json_data))

    # write to MQTT
    ret_code = mqtt_client.publish("sensor/meteo/"+str(sensor_id), payload=str(json_data), qos=0, retain=False)
    #print("MQTT return code: ", ret_code)

    if db_write:
        # write to DB
        sql = "INSERT IGNORE INTO meteo_sensor (temperature, humidity, pressure, ts, ts_epoch, sensor_id) VALUES " \
            "(%s, %s, %s, %s, %s, %s)"
        cursor.execute(sql, val)

    print("Temperature: %0.2f C" % temp)
    print("Humidity: %0.1f %%" % hum)
    print("Pressure: %0.1f hPa" % press)
#    print("Altitude = %0.2f meters" % bme280.altitude)
    print()
    return json_data


def on_publish(client,userdata,result):             #create function for callback
    print("data published ")
    print("result data: ", result)
    pass


def main():
    args = parseTheArgs()
    config = configparser.ConfigParser()
    config.read(args.f)

    try:
        periodicity = int(configSectionMap(config, "Sensor")['periodicity'])
    except:
        sys.exit("Periodicity value must be int")

    schedule.every(periodicity).seconds.do(job, config=config, args=args)
    while True:
        schedule.run_pending()
        time.sleep(5)



def job(config, args):
    try:
        conf_mqtt = configSectionMap(config, "MQTT")
    except:
        print("ERROR: Could not open config file, or could not find config section in file")
        config_full_path = os.getcwd() + "/" + args.f
        print("Tried to open the config file: ", config_full_path)
        sys.exit(1)

    cursor_DB = None
    if args.db_write:
        try:
            conf_db = configSectionMap(config, "DB")
        except:
            print("Could not open config file, or could not find config section in file")
            config_full_path = os.getcwd() + "/" + args.f
            print("Tried to open the config file: ", config_full_path)
            sys.exit(1)
        # connect DB
        try:
            mariadb_connection = mariadb.connect(host=conf_db['host'], port=conf_db['port'], user=conf_db['username'], password=conf_db['password'],
                                        database=conf_db['db'])
        except:
            print("ERROR: Could not connect to DB, exit")
            sys.exit(-1)
        cursor_DB = mariadb_connection.cursor()

    # connect MQTT
    mqtt_client = mqtt.Client(conf_mqtt['client_name'])
    mqtt_client.username_pw_set(conf_mqtt['username'], conf_mqtt['password'])
    mqtt_client.on_publish = on_publish
    try:
        mqtt_client.connect(conf_mqtt['host'])
    except:
        print("Can not connect to MQTT broker, exiting")
        #exit(-1)
    bme280_sensor = connectSensorBME280()
    getSensorData(bme280_sensor, mqtt_client, args.db_write, cursor_DB)
    if args.db_write:
        mariadb_connection.commit()
        cursor_DB.close()
        mariadb_connection.close()

    print("Disconnecting from MQTT")
    mqtt_client.disconnect()
    print("Bye")


# this is the standard boilerplate that calls the main() function
if __name__ == '__main__':
    # sys.exit(main(sys.argv)) # used to give a better look to exists
    rtcode = main()
    sys.exit(rtcode)
