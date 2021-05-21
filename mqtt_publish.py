#!/usr/bin/env python3

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import logging
import time
import json
import os



# Custom MQTT message callback

def customCallback(client, userdata, message):
    print("Received a new message: ")
    print(message.payload)
    print("from topic: ")
    print(message.topic)
    print("--------------\n\n")


# Set parameters

dirname = os.path.dirname(__file__)
dirCert = os.path.join(dirname, "certsAndKeys")

host = "a3ejkeh07lcisa-ats.iot.us-east-1.amazonaws.com"
rootCAPath = os.path.join(dirCert, "root-CA.crt")
certificatePath = os.path.join(dirCert, "bottling_plant.cert.pem")
privateKeyPath = os.path.join(dirCert, "bottling_plant.private.key")
port = 8883 #443
clientId = "machine_1"
topic = "test"
mode = "publish"


# Configure logging

logger = logging.getLogger("AWSIoTPythonSDK.core")
logger.setLevel(logging.DEBUG)
streamHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)


# Init AWSIoTMQTTClient

myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId)
myAWSIoTMQTTClient.configureEndpoint(host, port)
myAWSIoTMQTTClient.configureCredentials(rootCAPath, privateKeyPath, certificatePath)


# AWSIoTMQTTClient connection configuration

myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 128, 80)
myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
myAWSIoTMQTTClient.configureDrainingFrequency(100)  # Draining: 2 Hz
myAWSIoTMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
myAWSIoTMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec


# Connect to AWS IoT

myAWSIoTMQTTClient.connect()

