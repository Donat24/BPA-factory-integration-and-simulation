#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from os import EX_OSFILE, remove
import os
from arrow.api import get
import simpy
import arrow
import logging
import pandas as pd
import numpy as np
import json
import mqtt_publish as pub

#CONFIG
FILLING_SATIONS = 6
GENERATE_BOTTLE_TREASHOLD = 10
GENERATE_BOTTLES = 90
TIMESPAN_WAIT_BOTTLE = 10

#SETUP
TIME_FACTOR = 1.0 / 60 #Verhältniss von echten Sekudnen zur Simulation
START_DATE_TIME = "2021-05-03T07:29:00" #Anfgangsdatum der Simulation ... Ist wichtig für Scheduling

#Logging
logging.basicConfig(format="%(message)s", level=logging.DEBUG)

#pandas
status      = pd.read_excel("schedule.xlsx",sheet_name="Status",   converters={"Uhrzeit":str}).set_index("Uhrzeit")
maintenance = pd.read_excel("schedule.xlsx",sheet_name="Wartung",  converters={"Uhrzeit":str}).set_index("Uhrzeit")

#topic
IOT_TOPIC = os.environ.get("IOT_TOPIC", default="topic_1")

#--------------------------------------------------#
# Funktionen für Mqtt Publishing
#--------------------------------------------------#

def publish_event_message(machine, status, msg):
    global day_time
    utime = day_time.format("X")
    message = "".join([hex(machine), hex(status), hex(msg), hex(int(float(utime)))])
    #message = hex(int("".join([str(int(float(utime))), str(machine).zfill(3), 
    #    str(status), str(msg).zfill(2)])))     
    messageJson = json.dumps(message)
    pub.myAWSIoTMQTTClient.publish(IOT_TOPIC, messageJson, 1)

#--------------------------------------------------#
# Funktionen für Dauer
#--------------------------------------------------#

def timespan_generate_bottle():
    return max(np.random.normal(1,1),0.1)

def timespan_move_bottle_to_check():
    return max(np.random.normal(1, .002),0.1)

def timespan_check_bottle():
    return max(np.random.normal(.5, .002),0.1)

def timespan_remove_rejected_bottle():
    return max(np.random.normal(.25, .002),0.1)

def timespan_move_bottle_to_fill():
    return max(np.random.normal(1, .002),0.1)

def timespan_fill_bottles():
    return max(np.random.normal(5, .002),0.1)

def timespan_move_bottle_away():
    return max(np.random.normal(1, .002),0.1)

def timespan_maintenance(mean):
    return max(np.random.normal(mean, 10),60)

def timespan_issue_trigger():
    return max(np.random.normal(2 * 60 * 60, 10),60)

def timespan_repair_issue():
    return max(np.random.normal(5 * 60, 5),10)

#--------------------------------------------------#
# Funktionen für Chancen
#--------------------------------------------------#

# bad bottle wird aussortiert
def chance_bottle_rejected():
    return np.random.uniform(0,1) <= .01

# Maschinenfehler, muss händisch gefixed werden
def chance_bottle_issue():
    return np.random.uniform(0,1) <= .0005

#--------------------------------------------------#
# iot gedöns
#--------------------------------------------------#
def iot_status(status):
    logging.info(f"Status {status}")

def iot_bottle_filled(env):
    #logging.info("BOTTLE FILLED")
    update_time(env)
    publish_event_message(1,1,5)

def iot_bottle_rejected(env):
    #logging.info("BAD FILLED")
    update_time(env)
    publish_event_message(1,2,2)

def iot_beginn_maintenance(env):
    #logging.info("BEGINN MAINTENANCE")
    update_time(env)
    publish_event_message(1,1,3)

def iot_issue(env):
    #logging.info("IOT ERROR")
    update_time(env)
    publish_event_message(1,3,1)

def iot_repair_issue(env):
    #logging.info("REPAIRED")
    update_time(env)
    publish_event_message(1,1,4)

#--------------------------------------------------#
# Simulation
#--------------------------------------------------#
# STATUS
__running__ = False
__error__ = False

# TIME
start_day_time = arrow.get(START_DATE_TIME)
day_time = start_day_time

# aktuallisiert Zeit
def update_time(env):
    global startday
    global day_time

    day_time = start_day_time.shift(seconds=env.now)

#generiert Flaschen
def proc_generate_bottles(env,que_check):
    global __running__

    while __running__:
        if que_check.level < GENERATE_BOTTLE_TREASHOLD:
            for i in range(GENERATE_BOTTLES):
                yield env.timeout(timespan_generate_bottle())
                yield que_check.put(1)
        else:
            yield env.timeout(TIMESPAN_WAIT_BOTTLE)


#nimmt eine Flasche, überprüft diese und stellt diese zum Abfüllen bereit
def proc_check_bottles(env,que_check,que_fill,que_rejected):
    global __running__

    while __running__:
        yield que_check.get(1)
        yield env.timeout(timespan_move_bottle_to_check())
        yield env.timeout(timespan_check_bottle())

        if chance_bottle_rejected():
            yield env.timeout(timespan_remove_rejected_bottle())
            yield que_rejected.put(1)
            iot_bottle_rejected(env)
        else:
            yield env.timeout(timespan_move_bottle_to_fill())
            yield que_fill.put(1)

#Befüllt Flaschen
def proc_fill_bottles(env,res,que_fill,que_done):
    global __running__

    while __running__:
        try:
            with res.request(priority = 2) as req:
                yield req
                yield que_fill.get(FILLING_SATIONS)
                
                try:
                    yield env.timeout(timespan_fill_bottles())
                    
                    if chance_bottle_issue():
                        raise simpy.Interrupt(None)

                except simpy.Interrupt:
                    yield env.process(proc_repair_issue(env))

                
                finally:
                    yield env.timeout(timespan_move_bottle_away())
                    
                    for i in range(FILLING_SATIONS):
                        iot_bottle_filled(env)
                    
                    yield que_done.put(FILLING_SATIONS)
        
        except simpy.Interrupt:
            yield env.process(proc_repair_issue(env))

#Wartungsarbeiten
def proc_maintenance(env,minutes,res):
    with res.request(priority = 1) as req:
        yield req
        iot_beginn_maintenance(env)
        yield env.timeout(timespan_maintenance(minutes * 60))
        return

#Erezugt Probleme nach gewissen Zeitabständen
def proc_issue(env,proc):
    global __running__
    global __error__

    while __running__:
        yield env.timeout(timespan_issue_trigger())
        
        if __running__:
            if not __error__:
                try:
                    proc.interrupt()
                except:
                    pass

#Behebt Fehlers
def proc_repair_issue(env):
    global __error__
    global day_time

    update_time(env)
    logging.debug(f"{day_time} - Es liegt ein Fehler vor")
    __error__ = True
    iot_issue(env)
    yield env.timeout(timespan_repair_issue())
    update_time(env)
    logging.debug(f"{day_time} - Fehler behoben")
    __error__ = False
    iot_repair_issue(env)
    return

#Scheduling
def schedule(env):

    global __running__
    global day_time

    #Res symbolisiert die frei zur Verfügung stehende Abfüll-Anlage
    #Bei Wartungsarbeiten wird so die Maschine blockiert
    res = simpy.PriorityResource(env, capacity=1)

    #Queque
    que_check = simpy.Container(env,capacity=100)
    que_fill = simpy.Container(env,capacity=FILLING_SATIONS)
    que_done = simpy.Container(env)
    que_rejected = simpy.Container(env)

    while True:
        
        logging.debug(f"Datum:{day_time}|Queue_Check:{que_check.level}|Queue_Fill:{que_fill.level}|Queue_Done:{que_done.level}|Queue_Rej:{que_rejected.level}")

        weekday = day_time.isoweekday()
        time = day_time.format("HH:mm:ss")
        
        #Status
        if time in status.index:
            if not pd.isnull(status.loc[time][weekday]):
                __running__ = True if status.loc[time][weekday] == "r" else False
                iot_status(__running__)
                env.process(proc_generate_bottles(env,que_check))
                env.process(proc_check_bottles(env,que_check,que_fill,que_rejected))
                proc = env.process(proc_fill_bottles(env,res,que_fill,que_done))
                env.process(proc_issue(env,proc))
                
        
        #Wartung
        if time in maintenance.index:
            if not pd.isnull(maintenance.loc[time][weekday]):
                time = int(maintenance.loc[time][weekday])
                env.process(proc_maintenance(env,time,res))
        
        yield env.timeout(60)
        update_time(env)

#Startup Stuff
env = simpy.rt.RealtimeEnvironment(factor=TIME_FACTOR,strict=False)
env.process(schedule(env)) 
env.run()

pub.myAWSIoTMQTTClient.disconnect()
