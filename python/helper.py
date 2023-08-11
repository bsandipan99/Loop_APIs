import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np
from datetime import datetime
import pytz
from datetime import timezone
import time
import csv


### Database connection parameters
db_params = {
    "dbname": "sandipan",
    "user": "postgres",
    "host": "127.0.0.1",
    "port": "5432"
}


###### setting max time as  current_time ####### 
def find_current_time():
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
    
        # Setting max date as current date
        max_date_query = sql.SQL("SELECT MAX(date_time) FROM store_status")

        cursor.execute(max_date_query)
        current_time = cursor.fetchone()[0]  # Fetch the result of the query
        current_time = current_time.astimezone(pytz.utc)   # converting into utc
        
        cursor.close()
        connection.close()

        return current_time

    except Exception as e:
        print("Error: Unable to connect to the database.")
        print(e)
        exit()




###### time conversion helper functions #######
def calculate_seconds(source_time):

    # calculating time difference between local and UTC
    source_time = datetime.now(pytz.timezone(source_time))
    source_time = source_time.time()
    source_seconds = (source_time.hour * 3600) + (source_time.minute * 60) + source_time.second

    utc_time = datetime.now(pytz.utc)
    utc_time = utc_time.time()
    utc_seconds = (utc_time.hour * 3600) + (utc_time.minute * 60) + utc_time.second
    
    return utc_seconds - source_seconds


def convert_time():
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # for converting local_time to utc
        timezone_conversion = {}
        fetch_query = "SELECT * from store_timezone"

        cursor.execute(fetch_query)
        rows = cursor.fetchall()

        for row in rows:
            store_id, source_time = row
            timezone_conversion[store_id] = calculate_seconds(source_time)                # storing the timezone difference in seconds

        cursor.close()
        connection.close()

        return timezone_conversion

    except Exception as e:
        print("Error: Unable to connect to the database.")
        print(e)
        exit()




##### calculating total hours for each store per day of week ######
def store_total_hours(timezone_conversion):
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        
        total_hours = {}
        time_range = {}
        fetch_query = "SELECT * FROM menu_hours"        # fetching start_time and end_time

        cursor.execute(fetch_query)
        rows = cursor.fetchall()

        ## Iterating through the rows
        for row in rows:
            store_id, day, start_time, end_time = row
            start_time = datetime.strptime(str(start_time), "%H:%M:%S+05:30")
            end_time = datetime.strptime(str(end_time), "%H:%M:%S+05:30")


            # get difference between end time and start time
            delta = end_time - start_time
            hours = delta.total_seconds() / 3600

            key = (store_id,day)
            if key not in total_hours:
                total_hours[key] = hours
            else:
                total_hours[key] += hours

            if key not in time_range:
                time_range[key] = []
            
            start_time = start_time.time()
            start_time = (start_time.hour * 3600) + (start_time.minute * 60) + start_time.second 
            end_time = end_time.time()
            end_time = (end_time.hour * 3600) + (end_time.minute * 60) + end_time.second 

            if store_id not in timezone_conversion:
                timezone_conversion[store_id] = calculate_seconds('America/Chicago')	# timezone data missing for a store

            
            ## storing start_time - end_time of a store_id in a day of the week
            start_time += timezone_conversion[store_id]
            end_time += timezone_conversion[store_id]

            if start_time < 0 and end_time < 0:
                time_range[(store_id,(day + 6) % 7)].append(86400 + start_time)
                time_range[(store_id,(day + 6) % 7)].append(86400 + end_time)
            
            elif start_time < 0 and end_time >= 0:
                time_range[(store_id,(day + 6) % 7)].append(86400 + start_time)
                time_range[(store_id,(day + 6) % 7)].append(86400)
                time_range[key].append(0)
                time_range[key].append(end_time)
            else:
                time_range[key].append(start_time)
                time_range[key].append(end_time)
                
                
        cursor.close()
        connection.close()

        return timezone_conversion,  total_hours, time_range

    except Exception as e:
        print("Error: Unable to connect to the database.")
        print(e)
        exit()




#### checking whether status is within store open hours ####
def check(time_range,key,seconds):

    for i in range(1,len(time_range[key]),2):
        if seconds >= time_range[key][i-1] and seconds <= time_range[key][i]:
            return True
    
    return False




######### producing report ###########
def produce_report(downtime,day_of_week,total_hours,name):
    # Sample dictionary with values as lists of length 3

    # Define the CSV file path
    csv_file_path = '../output/' + name + '.csv'

    # Write the dictionary to the CSV file
    with open(csv_file_path, mode='w', newline='') as csv_file:
        fieldnames = ['store_id', 'uptime_last_hour(minutes)', 'uptime_last_day(hours)', 'uptime_last_week(hours)','downtime_last_hour(minutes)', 'downtime_last_day(hours)', 'downtime_last_week(hours)']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        # Write header
        writer.writeheader()

        # Write data rows
        for key, values in downtime.items():
            values[0] = format(values[0] / 60,".2f")
            values[1] = format(values[1] / 3600,".2f")
            values[2] = format(values[2] / 3600,".2f")

            # uptime last day
            uptime_day = 24 - float(values[1])                 
            if (key,day_of_week) in total_hours:
                uptime_day = total_hours[(key,day_of_week)] - float(values[1])
            uptime_day = format(uptime_day,".2f")

            # uptime last week
            sum = 0
            for i in range(0,7):
                if (key,i) in total_hours:
                    sum += total_hours[(key,i)]
                else:
                    sum += 24
            uptime_week = sum - float(values[2])               # uptime last week
            uptime_week = format(uptime_week,".2f")

            ## Writing in csv
            writer.writerow({
                'store_id': key,
                'uptime_last_hour(minutes)': 60 - float(values[0]),
                'uptime_last_day(hours)': uptime_day,
                 'uptime_last_week(hours)': uptime_week,
                'downtime_last_hour(minutes)': float(values[0]),
                'downtime_last_day(hours)': float(values[1]),
                'downtime_last_week(hours)': float(values[2]),

            })

#    print("CSV file created successfully!")


######### calculating downtime ##########
def calculate_downtime(current_time, total_hours,time_range,name):
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        last_time = {}
        downtime = {}

        day_of_week = current_time.weekday()            # finding day 
        current_time = (current_time - datetime(1970,1,1,tzinfo=timezone.utc)).total_seconds()
        
        ## fetching from database
        fetch_query = "SELECT * FROM store_status order by date_time desc"        

        cursor.execute(fetch_query)
        rows = cursor.fetchall()

        ## Iterating through the rows
        for row in rows:
            store_id, status, time_now = row

            time_now = time_now.astimezone(pytz.utc)   # converting into utc
            day_now = time_now.weekday()
            seconds_now = (time_now.hour * 3600) + (time_now.minute * 60) + time_now.second
            time_now = (time_now - datetime(1970,1,1,tzinfo=timezone.utc)).total_seconds()
            
            if store_id not in last_time:
                last_time[store_id] = [current_time, 'active']
            if store_id not in downtime:
                downtime[store_id] = [0,0,0]
            

            ######## last hour ########
            if current_time - time_now <= 3600:
                if status == 'inactive':
                    if (store_id,day_now) in time_range:                        # this key is present
                        if check(time_range,(store_id,day_now),seconds_now):               # check from stored timestamps
                            downtime[store_id][0] += int(min(last_time[store_id][0] - time_now,300))
                        else:
                            status = 'active'
                    else:
                        downtime[store_id][0] += int(min(last_time[store_id][0] - time_now,300))

                elif status == 'active' and last_time[store_id][1] == 'inactive':
                    downtime[store_id][0] += int(min(last_time[store_id][0] - time_now,300))            ## adding to downtime last_hour


            ######## last day ########
            if current_time - time_now <= 86400:
                if status == 'inactive':
                    if (store_id,day_now) in time_range:                        # this key is present
                        if check(time_range,(store_id,day_now),seconds_now):               # check from stored timestamps
                            downtime[store_id][1] += int(min(last_time[store_id][0] - time_now,300))
                        else:
                            status = 'active'
                    else:
                        downtime[store_id][1] += int(min(last_time[store_id][0] - time_now,300))
                    
                elif status == 'active' and last_time[store_id][1] == 'inactive':
                    downtime[store_id][1] += int(min(last_time[store_id][0] - time_now,300))            ## adding to downtime last_day
            

            ######## last week ########
            if current_time - time_now <= 604800:
                if status == 'inactive':
                    if (store_id,day_now) in time_range:                        # this key is present
                        if check(time_range,(store_id,day_now),seconds_now):               # check from stored timestamps
                            downtime[store_id][2] += int(min(last_time[store_id][0] - time_now,300))
                        else:
                            status = 'active'
                    else:
                        downtime[store_id][2] += int(min(last_time[store_id][0] - time_now,300))
                
                elif status == 'active' and last_time[store_id][1] == 'inactive':
                    downtime[store_id][2] += int(min(last_time[store_id][0] - time_now,300))            ## adding to downtime last_week
                        

            last_time[store_id] = [time_now, status]
#            time.sleep(0.5)
        
        # Writing into csv file
        produce_report(downtime,day_of_week,total_hours,name)


    except Exception as e:
        print("Error: Unable to connect to the database.")
        print(e)
        exit()



# # Calculate current_ time
# current_time = find_current_time()

# # Convert timezone in menu_hours
# timezone_conversion = convert_time()

# # Storing total hours of each store_id per day of the week
# timezone_conversion, total_hours, time_range = store_total_hours(timezone_conversion)

# # Calculating downtimes of last hour,day,week
# calculate_downtime(current_time, time_range)

