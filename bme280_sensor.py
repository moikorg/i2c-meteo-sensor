import sys
import board
import busio
import time
import adafruit_bme280
import mysql.connector as mariadb
from datetime import datetime
import json
import paho.mqtt.client as mqtt
import configparser


sensor_id = 2
temp_calibration = -0.4

def main():
    (conf_mqtt, conf_db) = getConfig()
    if not conf_db:
        print("ERROR: could not find the DB section in config file in the current directory: ", os.getcwd())
        return 1
    if not conf_mqtt:
        print("ERROR: could not find the MQTT section in config file in the current directory: ", os.getcwd())
        return 1

    # connect DB
    mariadb_connection = mariadb.connect(host=conf_db['host'], user=conf_db['username'], password=conf_db['password'],
                                     database=conf_db['db'])
    cursor_DB = mariadb_connection.cursor()

    # connect MQTT
    mqtt_client = mqtt.Client(conf_mqtt['client_name'])
    mqtt_client.username_pw_set(conf_mqtt['username'], conf_mqtt['password'])
    mqtt_client.connect(conf_mqtt['host'])

    bme280_sensor = connectSensorBME280()
    getSensorData(bme280_sensor, mqtt_client, cursor_DB)
    mariadb_connection.commit()
    cursor_DB.close()
    mariadb_connection.close()



def getConfig():
    config = configparser.ConfigParser()
    if not config.read('config.rc'):
       return None

    mqtt = {
        'host':config['MQTT']['host'],
        'username':config['MQTT']['username'],
        'password': config['MQTT']['password'],
        'client_name': config['MQTT']['client_name'],
    }
    db = {
        'host':config['DB']['host'],
        'password':config['DB']['password'],
        'username': config['DB']['username'],
        'db': config['DB']['db'],
    }
    return mqtt, db



def connectSensorBME280():
    # Create library object using our Bus I2C port
    sensor_address = 0x76
    i2c = busio.I2C(board.SCL, board.SDA)
    bme280 = adafruit_bme280.Adafruit_BME280_I2C(i2c, sensor_address)
    # change this to match the location's pressure (hPa) at sea level
    #bme280.sea_level_pressure = 1013.25
    return bme280


def getSensorData(sensor_bme280, mqtt_client, cursor):
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
    mqtt_client.publish("sensor/meteo/"+str(sensor_id), str(json_data))

    # write to DB
    sql = "INSERT IGNORE INTO meteo_sensor (temperature, humidity, pressure, ts, ts_epoch, sensor_id) VALUES " \
          "(%s, %s, %s, %s, %s, %s)"
    cursor.execute(sql, val)


    print("Temperature: %0.2f °C" % temp)
    print("Humidity: %0.1f %%" % hum)
    print("Pressure: %0.1f hPa" % press)
#    print("Altitude = %0.2f meters" % bme280.altitude)
    print()
    return json_data




# this is the standard boilerplate that calls the main() function
if __name__ == '__main__':
    # sys.exit(main(sys.argv)) # used to give a better look to exists
    rtcode = main()
    sys.exit(rtcode)