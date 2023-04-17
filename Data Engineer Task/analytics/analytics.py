from os import environ
from time import time, sleep
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, Integer, String, MetaData,TIMESTAMP,Date
import pandas as pd
from sqlalchemy.engine.reflection import Inspector
import datetime
import sys
import json
import math

# Write the solution here

def get_data(start,end,psql_engine):
    '''
        This function will return devices data from start to end range of time
    '''
    try:
        query = f"SELECT device_id,temperature,location,time FROM devices WHERE time between '{start}' AND '{end}'"
        with psql_engine.begin() as conn:
            df = pd.read_sql_query(sql=text(query), con=conn)
            print("The data has been found", len(df))
        return df
    except Exception as e:
        print(e)
   

def aggrigate_data(df):
    '''
        This function calculates the aggigrated Temprature, Datapoints and Total Distance covered of the dataframe supplied
    '''
    try:
        # Group by fethced data by devices
        grouped = df.groupby('device_id')

        # Aggrigating Temp, Datapoints & distance
        temp = grouped['temperature'].max()
        datapoint = grouped['time'].count()
        total_distance = grouped['location'].apply(lambda x: calculate_distance(x))

        # Union of result
        result = pd.concat([temp,datapoint,total_distance],axis=1)
        result = result.rename(columns={'location': 'total_distance','temperature': 'max_temperature', 'time': 'total_datapoints'})

        current_time = datetime.datetime.now()
        result['datetime'] = current_time.strftime('%Y-%m-%d')
        result['hour'] = int(current_time.strftime('%-H'))

        print("data aggrigated")
    except Exception as e:
        print(e)
    
    return result.reset_index()

def calculate_distance(locations):
    '''
        This function will calcualtes the distance cover from frist point to last point to the locations list provided.
    '''
    try:
        locations = locations.values.tolist()
        total_distance = 0.0
        
        for i in range(len(locations) - 1):
            try:
                loc1 = json.loads(locations[i])
                loc2 = json.loads(locations[i + 1])
                lat1, lon1 = map(math.radians, (float(loc1['latitude']), float(loc1['longitude'])))
                lat2, lon2 = map(math.radians, (float(loc2['latitude']), float(loc2['longitude'])))
                
                distance = math.acos(math.sin(lat1) * math.sin(lat2) + math.cos(lat1) * math.cos(lat2) * math.cos(lon2 - lon1)) * 6371
                
                total_distance += distance
            except Exception as e:
                print(e)
        
        print(f'distance calculated {total_distance}')
    except Exception as e:
        print(e)
    return total_distance

def create_table():
    try:
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        metadata_obj = MetaData()
        devices_aggrigator = Table(
            'devices_aggrigator', metadata_obj,
            Column('device_id', String(255), primary_key=True),
            Column('hour',Integer, primary_key=True),
            Column('datetime',Date, primary_key=True),
            Column('max_temperature', Integer),
            Column('total_datapoints', Integer),
            Column('total_distance', Integer),
        )
        inspector = Inspector.from_engine(mysql_engine)

        if not inspector.has_table('devices_aggrigator'):
            devices_aggrigator.create(mysql_engine)
    except OperationalError:
        sleep(0.1)

def insert_data(df):
    try:
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)

        query = generate_query('devices_aggrigator')
        with mysql_engine.connect() as conn:
            values = [tuple(x) for x in df[['device_id', 'datetime', 'hour', 'max_temperature', 'total_datapoints', 'total_distance']].values.tolist()]
            for value in values:
                conn.execute(text(query%value))
                conn.commit()
        print("data inserted")
    except Exception as e:
        print(e)

def generate_query(table_name):
    query = f"INSERT INTO {table_name} (device_id, datetime, hour, max_temperature, total_datapoints, total_distance) " \
            f"VALUES ('%s', '%s', %s, %s, %s, %s) " \
            f"ON DUPLICATE KEY UPDATE " \
            f"max_temperature = GREATEST(max_temperature, VALUES(max_temperature)), " \
            f"total_datapoints = total_datapoints +  VALUES(total_datapoints), " \
            f"total_distance = total_distance + VALUES(total_distance);"
    return query

def get_start_timestamp(psql_engine):
    query = "SELECT min(time) FROM devices;"
    with psql_engine.begin() as conn:
        df = pd.read_sql_query(sql=text(query), con=conn)
        df = df.dropna()
        if len(df) > 0:
            return int(df.values[0][0])
    return 0


def runner():
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        create_table()
        
        start = 0
        end = 0

        print('Waiting for data to be populate')
        sleep(20)

        while start == 0:
            start = get_start_timestamp(psql_engine)
            end = start + 1
            
        while True:
            df = get_data(start,end,psql_engine)
            data = aggrigate_data(df) 
            insert_data(data)
            start = end
            end = end + 5
            sys.stdout.flush()
            sleep(5)
    except Exception as e:
        print(e)
        sleep(0.1)

runner()

