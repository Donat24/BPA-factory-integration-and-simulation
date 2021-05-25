#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import simpy
import arrow
import logging
import pandas as pd
import numpy as np
import json
import mqtt_publish as pub

#SETUP
TIME_FACTOR = 1.0 / 60 #Verhältniss von echten Sekudnen zur Simulation
START_DATE_TIME = "2021-05-03T07:00:00" #Anfgangsdatum der Simulation ... Ist wichtig für Scheduling

#Logging
logging.basicConfig(format="%(message)s", level=logging.DEBUG)

#pandas
delivery    = pd.read_excel("schedule.xlsx",sheet_name="Lieferung",converters={"Uhrzeit":str}).set_index("Uhrzeit")
status      = pd.read_excel("schedule.xlsx",sheet_name="Status",   converters={"Uhrzeit":str}).set_index("Uhrzeit")
maintenance = pd.read_excel("schedule.xlsx",sheet_name="Wartung",  converters={"Uhrzeit":str}).set_index("Uhrzeit")


#--------------------------------------------------#
# Funktionen für Mqtt Publishing
#--------------------------------------------------#

def publish_event_message(machine, status, msg):
    message = "".join([str(machine).zfill(3), "-", str(status), str(msg).zfill(2)])
    messageJson = json.dumps(message)
    pub.myAWSIoTMQTTClient.publish("topic_1", messageJson, 1)


#--------------------------------------------------#
# Funktionen für Dauer
#--------------------------------------------------#

def timespan_bottle_to_queue():
    return np.random.normal(.1, .002)

def timespan_fill_bottle():
    return np.random.normal(1.3, .002)

def timespan_bad_bottle():
    return np.random.normal(.4, .002)

def timespan_error_trigger():
    return 60

def timespan_error_repair():
    return 60

#--------------------------------------------------#
# Funktionen für Chancen
#--------------------------------------------------#

# bad bottle wird aussortiert
def chance_bad_bottle():
    return np.random.uniform(0,1) <= .007

# Maschinenfehler, muss händisch gefixed werden
def chance_bottle_issue():
    return np.random.uniform(0,1) <= .0005

#--------------------------------------------------#
# iot gedöns
#--------------------------------------------------#
def iot_status(status):
    logging.info(f"Status {status}")

def iot_bottle_filled():
    #logging.info("BOTTLE FILLED")
    publish_event_message(1,1,5)

def iot_bad_bottle():
    #logging.info("BAD FILLED")
    publish_event_message(1,2,2)

def iot_beginn_maintenance():
    #logging.info("BEGINN MAINTENANCE")
    publish_event_message(1,1,3)

def iot_error():
    #logging.info("IOT ERROR")
    publish_event_message(1,3,1)

def iot_error_repair():
    logging.info("REPAIRED")
    publish_event_message(1,1,4)

#--------------------------------------------------#
# Simulation
#--------------------------------------------------#

#fügt Flasche in Warteschlange zum Befüllen
def proc_add_to_queue(env,number_of_bottles,que):
    for i in range(number_of_bottles):
        yield env.timeout(timespan_bottle_to_queue())
        que.put(1)

#Befüllt Flassche / Fertigt diese Ab
__running__ = False
__error__ = False
def proc_fill_bottle(env,res,que):
    global __running__
    global __error__

    iot_status(__running__)
    
    while __running__:
        if not __error__:
            with res.request(priority = 2) as req:
                yield req
                yield que.get(1)

                #Schlechte Flasche
                if chance_bad_bottle():
                    iot_bad_bottle()
                    yield env.timeout(timespan_bad_bottle())
                    continue
                
                #Flasche Abfertigen
                yield env.timeout(timespan_fill_bottle())
                iot_bottle_filled()

                #Flasche macht Probleme
                if chance_bottle_issue():
                    __error__ = True
        else:
            yield env.process(proc_error_repair(env))


#Wartungsarbeiten
def proc_maintenance(env,minutes,res):
    with res.request(priority = 1) as req:
        yield req
        iot_beginn_maintenance()
        yield env.timeout(minutes * 60)

#Es liegt ein Fehler vor
def proc_error(env,proc):
    global __error__
    while True:
        yield env.timeout(timespan_error_trigger())
        __error__ = True

#Beheben des Fehlers
def proc_error_repair(env,):
    global __error__
    iot_error()
    yield env.timeout(timespan_error_trigger())
    iot_error_repair()
    __error__ = False

#Scheduling
def schedule(env):

    global __running__
    #global __error__

    #Res symbolisiert die fre zur Verfügung stehende Abfüll-Anlage
    res = simpy.PriorityResource(env, capacity=1)

    #Que steht für die Anzahl an Flaschen die gerade befüllt werden können
    que = simpy.Container(env)

    #day ist ein Datumsobjekt welches zum simulieren der Wochentage genommen wird
    startday = arrow.get(START_DATE_TIME)
    day = startday

    while True:
        
        logging.debug(day)
        logging.debug(que.level)

        weekday = day.isoweekday()
        time = day.format("HH:mm:ss")
        
        #Lieferung
        if time in delivery.index:
            if not pd.isnull(delivery.loc[time][weekday]):
                amount = int(delivery.loc[time][weekday])
                logging.info(f"{amount} Flaschen geliefert")
                env.process(proc_add_to_queue(env,amount,que))
        
        #Status
        if time in status.index:
            if not pd.isnull(status.loc[time][weekday]):
                __running__ = True if status.loc[time][weekday] == "r" else False
                env.process(proc_fill_bottle(env,res,que))
                
        
        #Wartung
        if time in maintenance.index:
            if not pd.isnull(maintenance.loc[time][weekday]):
                time = int(maintenance.loc[time][weekday])
                env.process(proc_maintenance(env,time,res))
        
        yield env.timeout(60)
        day = startday.shift(seconds=env.now)

#Startup Stuff
env = simpy.rt.RealtimeEnvironment(factor=TIME_FACTOR,strict=False)
env.process(schedule(env)) 
env.run()

pub.myAWSIoTMQTTClient.disconnect()
