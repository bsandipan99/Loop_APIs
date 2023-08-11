import random
import string
import os
import threading
from helper import find_current_time, convert_time, store_total_hours, calculate_downtime
from helper import calculate_seconds, check, produce_report 

report_status = {}

# Generating random string and report
def trigger_report():

    # Creating random string
    os.system('clear')
    res = ''.join(random.choices(string.ascii_letters, k=15))

    print('Random string: ',res)
    input('\nPress enter to continue ')

    thread = threading.Thread(target=generate_report,args = (res,))
    thread.start()

def generate_report(res):
    report_status[res] = 1

    # Calculate current_ time
    current_time = find_current_time()

    # Convert timezone in menu_hours
    timezone_conversion = convert_time()

    # Storing total hours of each store_id per day of the week
    timezone_conversion, total_hours, time_range = store_total_hours(timezone_conversion)

    # Calculating downtimes of last hour,day,week
    calculate_downtime(current_time, total_hours, time_range,res)

    report_status[res] = 0


def get_report():
    os.system('clear')
    random_string = input('Enter the string ID: ')
    return_string = ""

    if report_status[random_string] == 1:
        return_string = "Running"
    else:
        return_string = "Complete"     
        
    print(return_string)
    input('\nPress enter to continue ')
    
    return return_string
