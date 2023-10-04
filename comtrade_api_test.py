import comtradeapicall
import json
import pandas as pd
import time
import random
import re
import os
'''
# Define codes for Peru and Argentina
PERU_CODE = '604'
ARGENTINA_CODE = '32'

base_wait_time = 30  # start with waiting for 30 seconds
max_wait_time = 600  # max wait time of 10 minutes
current_wait_time = base_wait_time
data = None
while data is None and (not isinstance(data, pd.DataFrame)):

    # Comercion total Arg-Peru
    data_peru_argentina = (
        comtradeapicall.previewFinalData( typeCode='C', freqCode='M', clCode='HS', period='202205',
        reporterCode=PERU_CODE, partnerCode=ARGENTINA_CODE, cmdCode='ALL', 
        flowCode='M', maxRecords=500, format_output='JSON', 
        aggregateBy=None, breakdownMode='classic', countOnly=None, includeDesc=True,
        partner2Code=None, customsCode=None, motCode=None )
    )

    time.sleep(current_wait_time)
    current_wait_time = min(current_wait_time + base_wait_time, max_wait_time)

    # Expo Inca Kola al mundo
    global 
    data_peru_exports_220210 = comtradeapicall.previewFinalData(
        typeCode='C', freqCode='M', clCode='HS', period='202205',
        reporterCode=PERU_CODE, partnerCode='0', cmdCode='220210', 
        flowCode='M', maxRecords=500, format_output='JSON', 
        aggregateBy=None, breakdownMode='classic', countOnly=None, includeDesc=True,
        partner2Code=None, customsCode=None, motCode=None
    )


    data = data_peru_argentina if data_peru_argentina is not None else data_peru_exports_220210
    time.sleep(current_wait_time)
    current_wait_time = min(current_wait_time + base_wait_time, max_wait_time)


# Save Peru to Argentina data
with open('data_peru_argentina.json', 'w') as file:
    json.dump(data_peru_argentina, file)

# Convert the JSON data to a pandas DataFrame and save as CSV
df_peru_argentina = pd.DataFrame(data_peru_argentina['dataset'])
df_peru_argentina.to_csv('data_peru_argentina.csv', index=False)

# Save Peru's exports of 220210 to the world data
with open('data_peru_exports_220210.json', 'w') as file:
    json.dump(data_peru_exports_220210, file)

# Convert the JSON data to a pandas DataFrame and save as CSV
df_peru_exports_220210 = pd.DataFrame(data_peru_exports_220210['dataset'])
df_peru_exports_220210.to_csv('data_peru_exports_220210.csv', index=False)

import comtradeapicall
data_peru_argentina_total = comtradeapicall.previewFinalData( 
                typeCode='C', freqCode='A', clCode='HS', period=f"2000",
                reporterCode=604, partnerCode=32, cmdCode='ALL', 
                flowCode='X', maxRecords=500, format_output='JSON', 
                aggregateBy=None, breakdownMode='classic', countOnly=None, 
                includeDesc=True, partner2Code=None, customsCode=None, motCode=None
            )
'''
##########################################################################################
os.chdir('/Users/fedelopez/Library/CloudStorage/OneDrive-Personal/Documents/UDESA/06/ECON_INTER/INCAKOLA/COMTRADE')
m=1
if not os.path.exists("data_peru_argentina_total"):
    os.makedirs("data_peru_argentina_total")
for y in range(2000, 2023):
    while True:
        data_peru_argentina_total = comtradeapicall.previewFinalData( 
            typeCode='C', freqCode='A', clCode='HS', period=f"{y}",
            reporterCode=604, partnerCode=32, cmdCode='AG6', 
            flowCode='X', maxRecords=2500, format_output='JSON', 
            aggregateBy=None, breakdownMode='plus', countOnly=None, 
            includeDesc=True, partner2Code=None, customsCode=None, motCode=None
        )
        if data_peru_argentina_total is None:
            time.sleep(7)
        else:
            df_temp = pd.DataFrame(data_peru_argentina_total)
            df_temp.to_csv(os.path.join("data_peru_argentina_total", 
                    f"data_peru_argentina_total_{y}.csv"), 
                    index=False) 
            break