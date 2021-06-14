#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from os import EX_OSFILE, remove
import os
from arrow.api import get
from pandas.tseries import offsets
import simpy
import arrow
import logging
import pandas as pd
import numpy as np
import json
import mqtt_publish as pub
import datetime

#CONFIG
FILLING_SATIONS = 6
GENERATE_BOTTLE_TREASHOLD = 10
GENERATE_BOTTLES = 90
TIMESPAN_WAIT_BOTTLE = 10

#SETUP
DEBUG           = bool(int(os.environ.get("DEBUG", default="0")))
TIME_FACTOR     = float(os.environ.get("TIME_FACTOR", default="1")) #Verhältnis von echten Sekunden zur Simulationszeit
START_DATE_TIME = os.environ.get("START_DATE_TIME", default="2021-05-03T07:29:00") #Anfangsdatum der Simulation für Scheduling

#Logging
logging.basicConfig(format="%(message)s", level=logging.DEBUG)

#pandas
status_excel      = pd.read_excel("schedule.xlsx",sheet_name="Status")
maintenance_excel = pd.read_excel("schedule.xlsx",sheet_name="Wartung")

#topic
IOT_TOPIC = os.environ.get("IOT_TOPIC", default="topic_1")

#--------------------------------------------------#
# Verarbeiten der eingelesenen Excel-Datein
#--------------------------------------------------#

#für Status Sheet
def compute_status_excel(df):

    res = pd.DataFrame(columns=["Tag","Start","Stop"])

    #Inhalt der Spalten
    time_col = df["Uhrzeit"]
    day_cols = [1,2,3,4,5,6,7]

    #Sucht Intervalle, in dennen die Maschine eingeschalten ist
    for day in day_cols:
        schedule = df.loc[:,day]
        starts = time_col[schedule == "r"]
        stops  = time_col[schedule == "s"]
        for index, start in starts.iteritems():
            next_stop = stops[stops > start].iloc[0]
            res = res.append({
                "Tag" : day,
                "Start": start,
                "Stop": next_stop
            }, ignore_index=True)
    
    return res

#dür Manintenance Sheet
def compute_maintenance_excel(df):

    res = pd.DataFrame(columns=["Tag","Start","Dauer"])

    #Inhalt der Spalten
    time_col = df["Uhrzeit"]
    day_cols = [1,2,3,4,5,6,7]

    #Sucht Intervalle, in dennen die Maschine eingeschalten ist
    for day in day_cols:
        schedule = df.loc[:,day]
        starts = time_col[pd.notna(schedule)]
        for index, start in starts.iteritems():
            res = res.append({
                "Tag" : day,
                "Start": start,
                "Dauer": schedule[index]
            }, ignore_index=True)
    
    return res


status = compute_status_excel(status_excel)
maintenance = compute_maintenance_excel(maintenance_excel)

del(status_excel)
del(maintenance_excel)

# Hilfsfunktion
def to_time(minutes=0):
    return (datetime.datetime(2000,1,1) + datetime.timedelta(minutes=minutes)).time()

#--------------------------------------------------#
# erhalte Status
#--------------------------------------------------#

# Schaut in der Schdule nach ob die maschine gerade laufen sollte
def should_run():

    global day_time

    time = day_time.time()
    day = day_time.isoweekday()
    return ((status.Tag == day) & (status.Start_updated >= time) & (status.Stop_updated <= time)).any()

#beendet oder statet Maschine nach Zeitplan
def check_status(env,res,que_check,que_fill,que_done,que_rejected,que_removed):

    global __running__

    if should_run():
        if not __running__:
            env.process(proc_start_processes(env,res,que_check,que_fill,que_done,que_rejected,que_removed))
    else:
        if __running__:
            env.process(proc_end_processes(env))

#Startet Wartung nach Zeitplan
def check_maintenance(env,res):

    global day_time

    time = day_time.time()
    day = day_time.isoweekday()

    maintenance_slice = ((maintenance.Tag == day) & (maintenance.Start_updated == time))
    if maintenance_slice.any():
        mins = maintenance[maintenance_slice]["Dauer"][0]
        env.process(proc_maintenance(env,mins,res))

#--------------------------------------------------#
# Funktionen für Mqtt Publishing
#--------------------------------------------------#

def publish_event_message(machine, status, msg):
    global day_time
    utime = day_time.format("X")
    timestamp = int(float(utime))
    arrtime_array = timestamp.to_bytes(4, 'big')
    uint8 = bytearray(7)
    uint8[0] = machine
    uint8[1] = status
    uint8[2] = msg
    uint8[3] = arrtime_array[0];
    uint8[4] = arrtime_array[1];
    uint8[5] = arrtime_array[2];
    uint8[6] = arrtime_array[3];

    hexadecimal_string = uint8.hex()
    messageJson = json.dumps(hexadecimal_string)  
    pub.myAWSIoTMQTTClient.publish(IOT_TOPIC, messageJson, 1)

#--------------------------------------------------#
# Funktionen für Dauer
#--------------------------------------------------#

#variabler Anfang
def time_start_machine(time_in_minutes):
    return np.random.normal(time_in_minutes, 7)

def time_stop_machine(time_in_minutes):
    return np.random.normal(time_in_minutes, 3)

def time_maintenance(time_in_minutes):
    return np.random.normal(time_in_minutes, 3)

def timespan_offset():
    return np.random.uniform(0,60)

def update_status_times():
    status["Start_updated"] = status.Start.apply(lambda x: to_time(time_start_machine(x.hour * 60 + x.minute - 1)))
    status["Start_updated"] = status.Start_updated.apply(lambda x: x.replace(second=0,microsecond=0))
    status["Stop_updated"] = status.Stop.apply(lambda x: to_time(time_stop_machine(x.hour * 60 + x.minute - 1)))
    status["Stop_updated"] = status.Stop_updated.apply(lambda x: x.replace(second=0,microsecond=0))

def update_maintenance_times():
    maintenance["Start_updated"] = maintenance.Start.apply(lambda x: to_time(time_start_machine(x.hour * 60 + x.minute)))
    maintenance["Start_updated"] = maintenance.Start_updated.apply(lambda x: x.replace(second=0,microsecond=0))

#variable Dauer
def timespan_generate_bottle():
    return max(np.random.normal(1,1),0.1)

def timespan_move_bottle_to_check():
    return max(np.random.normal(.2, .002),0.1)

def timespan_check_bottle():
    return max(np.random.normal(.5, .002),0.1)

def timespan_remove_rejected_bottle():
    return max(np.random.normal(.25, .002),0.1)

def timespan_move_bottle_to_fill():
    return max(np.random.normal(.2, .002),0.1)

def timespan_fill_bottles():
    return max(np.random.normal(5, .002),0.1)

def timespan_move_bottle_away():
    return max(np.random.normal(.16, .002),0.1)

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

def chance_bottle_remove():
    return np.random.uniform(0,1) <= .5

#--------------------------------------------------#
# iot stuff
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
start_day_time = arrow.get(START_DATE_TIME) if DEBUG else arrow.now()
day_time = start_day_time

# aktuallisiert Zeit
def update_time(env):
    global startday
    global day_time

    day_time = start_day_time.shift(seconds=env.now)

#Startet alle Prozesse zum Arbeitsbeginn
def proc_start_processes(env,res,que_check,que_fill,que_done,que_rejected,que_removed,offset = 0):
    
    global __running__

    
    #Verzgert dn Anfang
    if offset:
        yield env.timeout(offset)
    else: 
        yield env.timeout(timespan_offset)

    #verhindert das mehrfache startens
    if __running__:
        return

    __running__ = True
    iot_status(__running__)
    env.process(proc_generate_bottles(env,que_check))
    env.process(proc_check_bottles(env,que_check,que_fill,que_rejected))
    env.process(proc_fill_bottles(env,res,que_fill,que_done,que_removed))
    env.process(proc_issue(env))

#beendet alle Prozesse wenn der Arbeitstag vorbei ist
def proc_end_processes(env,offset = 0):
    global __running__

    #Verzgert das Ende
    if offset:
        yield env.timeout(offset)
    else: 
        yield env.timeout(timespan_offset)

    __running__ = False
    iot_status(__running__)

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
def proc_fill_bottles(env,res,que_fill,que_done,que_removed):
    global __running__
    global __error__

    while __running__:

        remove_bottles = 0

        with res.request(priority = 2) as req:
            
            yield req
            yield que_fill.get(FILLING_SATIONS)

            #Falls beim Warten auf die Flaschen ein Fehler auftritt
            if __error__:
                yield env.process(proc_repair_issue(env))

            yield env.timeout(timespan_fill_bottles())
            
            #Falls beim Abfüllen ein Fehler auftritt
            if chance_bottle_issue():
                __error__ = True
                yield env.process(proc_repair_issue(env))
                
                #GGF ist eine Flasche doch Kaputt und muss händisch entfernt werden
                if chance_bottle_remove():
                    remove_bottles = 1
                    que_removed.put(remove_bottles)
            
            #abtransport der Flaschen   
            for i in range(FILLING_SATIONS - remove_bottles):
                yield env.timeout(timespan_move_bottle_away())
                iot_bottle_filled(env)
            
            yield que_done.put(FILLING_SATIONS)

#Wartungsarbeiten
def proc_maintenance(env,minutes,res,offset = 0):

    #Verzögert Wartung
    if offset:
        yield env.timeout(offset)
    else: 
        yield env.timeout(timespan_offset)

    with res.request(priority = 1) as req:
        yield req
        iot_beginn_maintenance(env)
        yield env.timeout(timespan_maintenance(minutes * 60))
        return

#Erzeugt Probleme nach zufälligen Zeitabständen
def proc_issue(env):
    global __running__
    global __error__

    while __running__:
        yield env.timeout(timespan_issue_trigger())
        
        if __running__:
            __error__ = True

#Behebt Fehler
def proc_repair_issue(env):
    global __error__
    global day_time

    #Vor der Reperatur
    update_time(env)
    logging.debug(f"{day_time} - Es liegt ein Fehler vor")
    __error__ = True
    iot_issue(env)
    
    #Reperatur
    yield env.timeout(timespan_repair_issue())
    
    #Danach
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
    que_removed = simpy.Container(env)

    #Varianz für die erste Woche
    update_status_times()
    update_maintenance_times()

    while True:

        logging.debug(f"Datum:{day_time}|Queue_Check:{que_check.level}|Queue_Fill:{que_fill.level}|Queue_Done:{que_done.level}|Queue_Rej:{que_rejected.level}")
       
        time = day_time.time()
        day = day_time.isoweekday()

        #lässt das ein und ausschalten etwas variieren
        if(day == 7 and time == datetime.time(23,59,00)):
            update_status_times()
            update_maintenance_times()

        #Status
        check_status(env,res,que_check,que_fill,que_done,que_rejected,que_removed)
        
        #Wartung
        check_maintenance(env,res)
        
        yield env.timeout(60)
        update_time(env)

#Startup Stuff
env = simpy.rt.RealtimeEnvironment(factor=TIME_FACTOR,strict=False)
env.process(schedule(env)) 
env.run()

pub.myAWSIoTMQTTClient.disconnect()
