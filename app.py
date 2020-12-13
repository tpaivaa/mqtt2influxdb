#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import datetime
import time
import json
from soittilaconf import c as config
from influxdb import InfluxDBClient
import logging

def on_connect(client, userdata, flags, rc):
    logging.info("Connected with result code "+str(rc))
    client.subscribe([("home/sensor/raspi/#",1),("home/ykmakuuhuone/#",1)])

def on_message(client, userdata, msg):
    try:
      logging.info("Received a message on topic: " + msg.topic)
      # Use utc as timestamp
      receiveTime=datetime.datetime.utcnow()
      message=json.loads(msg.payload.decode("utf-8"))
      isfloatValue=False
      if msg.topic == 'home/ykmakuuhuone/sensor':
        _message = json.loads(msg.payload)
        measurement = 'ykmakuuhuoneHumi'
        val = round(float(_message['Humidity']),2)
        isfloatValue=True
      else:
        measurement=msg.topic.split('/raspi/')[1].split('/')[0]
        #print(message)
        #print(message["Temp"])
        # Convert the string to a float so that it is stored as a number and not a string in the database
        val = round(float(message["Temp"]),2)
        #print("Value:"+str(val))
        isfloatValue=True
    except:
        logging.info("Could not convert " + measurement + " to a float value")
        isfloatValue=False

    if isfloatValue:
        logging.info(str(receiveTime) + ": " + msg.topic + " " + str(val))

        json_body = [
            {
                "measurement": measurement,
                "time": receiveTime,
                "fields": {
                    "value": val
                }
            }
        ]

        dbclient.write_points(json_body)
        logging.info("Finished writing to InfluxDB")

# Set up a client for InfluxDB
dbclient = InfluxDBClient(config['influxDBServer'], 8086, config['influxDBUser'], config['influxDBPassword'], config['influxDBdatabase'])
userName = config['mqttUser']
passWord = config['mqttPassword']
# Initialize the MQTT client that should connect to the Mosquitto broker
client = mqtt.Client()
client.username_pw_set(userName,passWord)
client.on_connect = on_connect
client.on_message = on_message
connOK=False
while(connOK == False):
    logging.info('trying to connect')
    try:
        client.connect(config['mqttServer'], 1883, 60)
        connOK = True
        logging.info('Connected')
    except:
        connOK = False
    time.sleep(2)

# Blocking loop to the Mosquitto broker
client.loop_forever()