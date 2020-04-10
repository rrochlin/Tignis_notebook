# Databricks notebook source
import pandas as pd
import os
import matplotlib.pyplot as plt
import glob

# COMMAND ----------

#gathering csv data using glob
all_csv_files = glob.glob("/dbfs/FileStore/tables/Auto/*.csv")
print("--" + str(all_csv_files))
loops=len(all_csv_files)
all_csv_files

# COMMAND ----------

#TS2Sec function def. converts datestamps from the crown point building to seconds from jan 2019.
def monthstime(month):
        if month =='Jan':  
            return 0
        elif month == 'Feb':  
            return 31
                     
        elif month ==  'Mar':  
            return 31+28
                   
        elif month == 'Apr':  
            return 31+28+31
                     
        elif month == 'May':  
            return 31+28+31+30
                     
        elif month == 'Jun':  
            return 31+28+31+30+31
                     
        elif month == 'Jul':  
            return 31+28+31+30+31+30
                     
        elif month == 'Aug':  
            return 31+28+31+30+31+30+31
                     
        elif month == 'Sep':  
            return 31+28+31+30+31+30+31+31
                     
        elif month == 'Oct': 
            return 31+28+31+30+31+30+31+31+30
                     
        elif month == 'Nov': 
            return 31+28+31+30+31+30+31+31+30+31
                     
        elif month == 'Dec': 
            return 31+28+31+30+31+30+31+31+30+31+30
                     
        



def TS2Sec(x):
    loops=len(x)
    for i in range(loops):
        total=0
        month=x.iat[i,0]
        #days month is dd-MMM-YY HH:mm:ss am or pm
        temp=month.split('-')[0]
        month=month.split('-')[1]+'-'+month.split('-')[2]
        total+=int(temp)*24*60**2
        #months month is MMM-YY HH:mm:ss am or pm
        temp=month.split('-')[0]
        month=month.split('-')[1]
        total+=monthstime(temp)*24*60**2
        #year month is YY HH:mm:ss am or pm
        temp=month.split(' ')[0]
        month=month.split(' ')[1]+' '+month.split(' ')[2]
        total+=(int(temp)-18)*365*24*60**2
        #hours month is HH:mm:ss am or pm
        temp=month.split(':')[0]
        month=month.split(':')[1]+':'+month.split(':')[2]
        total+=int(temp)*60**2
        #minutes month is mm:ss am or pm
        temp=month.split(':')[0]
        month=month.split(':')[1]
        total+=int(temp)*60
        #seconds month is ss am or pm
        temp=month.split(' ')[0]
        month=month.split(' ')[1]
        total+=int(temp)*60
        #am/pm month is am or pm
        if month == 'pm':
            total+=12*60**2
        x.iat[i,0]=total

# COMMAND ----------

import numpy as np
varlist=[]

for i in range(int(loops)):
    if i == int(loops-1):
      globals() ['df'+str(i)] = pd.read_csv(all_csv_files[0])
    else:
      globals() ['df'+str(i)] = pd.read_csv(all_csv_files[int(i+1)])
    if i == 0:
      Result = df0
    else:
      Result = Result.merge(globals() ['df'+str(i)], on='Timestamp',how='left')
      
Result

# COMMAND ----------

ax = plt.gca()
Result.plot(kind='line',x='Timestamp',y='RTU_1 OutdoorTemp(°F)',color='blue',ax=ax)
Result.plot(kind='line',x='Timestamp',y='RTU_1 RATemp(°F)', color='red', ax=ax)
plt.xticks(rotation='vertical')

#here we're looking at retrun air vs outdoor temp
#anywhere where OutDoorTemp>RATemp we expect the 
#economizer to be active
display(ax)


# COMMAND ----------

|