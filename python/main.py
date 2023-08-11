import random
import string
import os
import threading
from helper import find_current_time, convert_time, store_total_hours, calculate_downtime
from helper import calculate_seconds, check, produce_report 
from api import report_status, trigger_report, get_report

while True:
    os.system('clear')

    print("1) Generate String")
    print("2) Check Status")
    print("3) Exit\n")
    
    option = int(input("Select an option: "))

    if option == 1:
        trigger_report()
    elif option == 2:
        get_report()
    else:
        break


