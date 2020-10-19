import requests
import datetime
import json
import dateutil.parser
import traceback
import time
import argparse

parser = argparse.ArgumentParser(
    description='Script to download data from Airella services and send them to AGH Infrastructure')
parser.add_argument(
    '--stations', help='List of stations ids, splited by comma')
parser.add_argument('--email', help='Email of account at Airella')
parser.add_argument('--password', help='Password of account at Airella')
parser.add_argument('--airella-api-url', help='Airella API URL')
parser.add_argument('--agh-api-url', help='AGH API URL')
parser.add_argument('--agh-api-token', help='AGH API token')

args = parser.parse_args()

stations = args.stations.split(',')
email = args.email
password = args.password
airella_api_url = args.airella_api_url
agh_api_url = args.agh_api_url
agh_api_token = args.agh_api_token

access_token = None
refresh_token = None

stations_last_heartbeat = {}


def login():
    global access_token
    global refresh_token
    r = requests.post(airella_api_url + "/auth/login", json={
        "email": email,
        "password": password
    })
    if r.status_code != 200:
        raise RuntimeError("Error when logging in")
    access_token = r.json()["data"]["accessToken"]
    refresh_token = r.json()["data"]["refreshToken"]


def refresh_access_token():
    global access_token
    r = requests.post(airella_api_url + "/auth/refresh-token", json={
        "refreshToken": refresh_token,
    })
    if r.status_code != 200:
        raise RuntimeError("Error refreshing token")
    access_token = r.json()["data"]["accessToken"]


def make_authorized_GET_request(url):
    return requests.get(url, headers={
        'Content-Type': 'application/json',
        "Authorization": "Bearer {}".format(access_token["token"])
    })


def get_user_station_ids():
    url = airella_api_url + "/user/stations"
    stations = make_authorized_GET_request(url).json()["data"]
    station_ids = map(lambda station: station["id"], stations)
    return station_ids


def get_sensor_last_measurement(station_id, sensor_id):
    url = airella_api_url + "/stations/{}/sensors/{}".format(
        station_id, sensor_id)
    values = make_authorized_GET_request(url).json()["data"]["values"]
    if len(values) == 0:
        return None
    return values[0]


def get_station_location(station_id):
    url = airella_api_url + "/stations/{}".format(station_id)
    return make_authorized_GET_request(url).json()["data"]["location"]


def get_station_address(station_id):
    url = airella_api_url + "/stations/{}".format(station_id)
    return make_authorized_GET_request(url).json()["data"]["address"]


def get_statistic_last_value(station_id, statistic_id):
    url = airella_api_url + "/stations/{}/statistics/{}".format(
        station_id, statistic_id)
    values = make_authorized_GET_request(url).json()["data"]["values"]
    if len(values) == 0:
        return None
    return values[0]


def get_station_data(station_id):
    return {
        "pm1": get_sensor_last_measurement(station_id, "pm1"),
        "pm2_5": get_sensor_last_measurement(station_id, "pm2_5"),
        "pm10": get_sensor_last_measurement(station_id, "pm10"),
        "temperature": get_sensor_last_measurement(station_id, "temperature"),
        "humidity": get_sensor_last_measurement(station_id, "humidity"),
        "pressure": get_sensor_last_measurement(station_id, "pressure"),
        "address":  get_station_address(station_id),
        "location":  get_station_location(station_id),
        "busVoltage": get_statistic_last_value(station_id, "busVoltage"),
        "heaterTemp": get_statistic_last_value(station_id, "heaterTemp"),
        "heaterHum": get_statistic_last_value(station_id, "heaterHum"),
        "heaterPower": get_statistic_last_value(station_id, "heaterPower"),
        "heaterState": get_statistic_last_value(station_id, "heaterState"),
        "heaterDewPoint": get_statistic_last_value(station_id, "heaterDewPoint"),
        "heartbeat": get_statistic_last_value(station_id, "heartbeat"),
        "current": get_statistic_last_value(station_id, "current"),
    }


def check_station_data(station_id, station_data):
    global stations_last_heartbeat
    if station_data["heartbeat"] == None:
        raise RuntimeError(
            "Station {} has heartbeat value, station isn't working")

    if "heartbeat" in stations_last_heartbeat:
        last_heartbeat_timestamp = dateutil.parser.parse(
            stations_last_heartbeat[station_id])
        next_heartbeat_timestamp = dateutil.parser.parse(
            station_data["heartbeat"]["timestamp"])
        if next_heartbeat_timestamp <= last_heartbeat_timestamp:
            raise RuntimeError(
                "Station {} hasn't been updated since last time check".format(station_id))
    stations_last_heartbeat[station_id] = station_data["heartbeat"]["timestamp"]

    for data_key in station_data:
        if station_data[data_key] is None:
            raise RuntimeError(
                "Station {} has null {} value".format(station_id, data_key))


def prepare_station_data(station_id, station_data):
    return {
        "stationId": station_id,
        "locName": "{}, {}, {} {}".format(station_data["address"]["country"], station_data["address"]["city"],
                                          station_data["address"]["street"], station_data["address"]["number"]),
        "sensors": {
            "mainSensorModule": {
                "meta": {
                    "softwareWatchdog": {"active": True, "value": 0},
                    # control register current state or from the time the data was collected
                    "controlRegister": 0,
                    "i2cInhibit": False,
                    "rtc": {
                        "present": False,
                        "dataValid": True,
                        "modulePresent": True,
                        "model": "DS3231",
                        # timestamp at which the data in the sensorData section had been collected valid only if dataOk=True in readable and unix format
                        "timestamp": {"iso": station_data["heartbeat"]["timestamp"]}
                    },
                    "offlineFeatures": {
                        "offlineMode": False,  # is sensor station currently in offline mode?
                        "regBlock": 0  # block in which the data had been stored and from which it has been retrieved
                    }
                },

                "sensorData": {
                    "power": {
                        "sbcPowerOn": False,
                        "reg5VOn": False,
                        "supplyVoltage": station_data["busVoltage"]["value"]},
                    "gps": {"model": "ORG1510-R01", "powerOn": False, "dataValid": False, "latitude": station_data["location"]["latitude"], "longitude": station_data["location"]["longitude"]},

                    "heater": {"powerOn": station_data["heaterState"]["value"] == "ON", "powerLevel": station_data["heaterPower"]["value"], "tempRead": station_data["heaterTemp"]["value"]},

                    "particleConcentrationSensor": {
                        "model": "PMS7003",
                        "powerOn": True,
                        "averageCurrentDraw": station_data["current"]["value"],
                        "concentration": {
                            "atmoPressAverage": {"pm1": station_data["pm1"]["value"], "pm2_5": station_data["pm2_5"]["value"], "pm10": station_data["pm10"]["value"]},
                        },
                    },

                    "envSensor": {"model": "BME280", "dataValid": False, "dewPoint": station_data["heaterDewPoint"]["value"], "relativeHumidity": station_data["humidity"]["value"], "temperature": station_data["temperature"]["value"], "pressure": station_data["pressure"]["value"]}
                }

            }
        }
    }


def send_station_data(station_id, data):
    txpayload = {
        "label": "Airella Quality Sensor",
        "timestamp": datetime.datetime.now().replace(microsecond=0).isoformat(),
        "data": json.dumps(data),
    }

    urlData = agh_api_url
    headersData = {"token": agh_api_token}
    r = requests.post(urlData, data=txpayload, headers=headersData, timeout=10)
    if r.status_code != 201:
        raise RuntimeError(
            "Error when sending data, status code: {}".format(r.status_code))


def send_all_stations_data():
    stations_to_send_data = stations
    if len(stations_to_send_data) == 0:
        stations_to_send_data = get_user_station_ids()
    for station_id in stations_to_send_data:
        try:
            print("Getting data about station {}".format(station_id))
            data = get_station_data(station_id)
            check_station_data(station_id, data)
            request_data = prepare_station_data(station_id, data)
            send_station_data(station_id, request_data)
            print("Sent data about station {}".format(station_id))
        except:
            traceback.print_exc()
            continue


def main():
    login()
    while True:
        refresh_access_token()
        send_all_stations_data()
        print("Next interation will be after 5 minutes")
        time.sleep(5 * 1000 * 60)  # 5 minutes


main()
