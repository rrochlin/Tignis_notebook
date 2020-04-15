# Databricks notebook source
import pandas as pd
import os
import matplotlib.pyplot as plt
import glob

# COMMAND ----------

#gathering csv data using glob
all_csv_files = glob.glob("/dbfs/FileStore/tables/2019/*.csv")
print("--" + str(all_csv_files))
loops=len(all_csv_files)

# COMMAND ----------

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
                     
        


#takes x the data frame with timestamp in column 0, and y a blank data frame 1 colum x len(x) rows to be appended to x at the end
def TS2Sec(x):
    loops=len(x)
    for i in range(loops):
        total=0
        month=x.iloc[i,0]
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
        x.iloc[i,0]=total


# COMMAND ----------

for i in range(int(loops)):
    
    if i == int(loops-1):
      globals() ['df'+str(i)] = pd.read_csv(all_csv_files[0])
      
    else:
      globals() ['df'+str(i)] = pd.read_csv(all_csv_files[int(i+1)])
      
    if i == 0:
      
      Result = df0
      TS2Sec(Result)

    else:
      TS2Sec(globals() ['df'+str(i)])
      Result = Result.merge(globals() ['df'+str(i)], on=['Timestamp'],sort=True,how='outer')


# COMMAND ----------

for i in range(int(loops)):
    
    if i == int(loops-1):
      globals() ['df'+str(i)] = pd.read_csv(all_csv_files[0])
      
    else:
      globals() ['df'+str(i)] = pd.read_csv(all_csv_files[int(i+1)])
      
    if i == 0:
      
      Result = df0
      

    else:
      Result = Result.merge(globals() ['df'+str(i)], on=['Timestamp'],sort=True,how='outer')

# COMMAND ----------

ax = plt.gca()
Result2.plot(kind='line',x='Timestamp',y='RTU_1 OutdoorTemp(°F)',color='blue',ax=ax)
Result2.plot(kind='line',x='Timestamp',y='RTU_1 RATemp(°F)', color='red', ax=ax)
plt.xticks(rotation='vertical')
#ax.set_xlim('18-May-18','18-Apr-20')

#here we're looking at retrun air vs outdoor temp
#anywhere where OutDoorTemp>RATemp we expect the 
#economizer to be active
display(ax)

# COMMAND ----------

#adjust parameters of faultcheck
toleranceTic=0
toleranceTemp=0

# COMMAND ----------

#faultcheck logic
loops2 = len(Result)
FaultStamps = pd.DataFrame([], columns=['Timestamp','Fault','Status','OAT','RAT','delT'])
tolerance = toleranceTic
if Result['RTU_1 OutdoorTemp(°F)'][0]>Result['RTU_1 RATemp(°F)'][0]:
  Status='Enabled'
else:
  Status ="OffAmb"
StatusRef="20"
print(Status)
checkerOAT=0
checkerRAT=0


for i in range(loops2):
  #checks if theres an update to EconoStatus
  if not pd.isnull(Result['RTU_1 EconoStatus'][i]):
    Status=Result['RTU_1 EconoStatus'][i]
    StatusRef=" "+str(i)
  #checks for updates OAT
  if not pd.isnull(Result['RTU_1 OutdoorTemp(°F)'][i]):
    OAT=Result['RTU_1 OutdoorTemp(°F)'][i]
    checkerOAT = 0
  else:
    checkerOAT+=1
    
  #checks for updates RAT  
  if not pd.isnull(Result['RTU_1 RATemp(°F)'][i]):
    RAT=Result['RTU_1 RATemp(°F)'][i] 
    checkerRAT=0
  else:
    checkerRAT+=1
  
  #only runs checks on intervals we have data for
  if not (pd.isnull(Result['RTU_1 EconoStatus'][i])):
    
    
  #compares most recent RAT to OAT  
    if RAT > OAT+toleranceTemp:
      if not (Status == "Enabled"):
        #adjustable tolerance options set to none right now
        tolerance += 1
        
        if tolerance > 1:
          fault=pd.DataFrame([[Result['Timestamp'][i],'Failed to Activate',Status + StatusRef,str(OAT)+' || '+str(checkerOAT),str(RAT)+' || '+str(checkerRAT),RAT-OAT]], columns=['Timestamp','Fault','Status','OAT','RAT','delT'])
          FaultStamps=FaultStamps.append(fault,ignore_index=True)
          
      else:
        tolerance = toleranceTic;
      
      
    elif RAT < OAT+toleranceTemp:
      if not (Status == "OffAmb"):
        tolerance += 1
        if tolerance > 1:
          fault=pd.DataFrame([[Result['Timestamp'][i],'Deactivation Failed',Status + StatusRef,str(OAT)+' || '+str(checkerOAT),str(RAT)+' || '+str(checkerRAT),RAT-OAT]], columns=['Timestamp','Fault','Status','OAT','RAT','delT'])
          FaultStamps=FaultStamps.append(fault,ignore_index=True)
          
        else:
          tolerance = toleranceTic;

# COMMAND ----------

FaultStamps

# COMMAND ----------

jam=0
for i in range(loops2):
  if i>0:
    if Result['Timestamp'][i] < Result['Timestamp'][i-1]:
      jam+=1
jam

# COMMAND ----------

for i in range(loops2):
  if not pd.isnull(Result['RTU_1 OutdoorTemp(°F)'][i]):
    OAT=Result['RTU_1 OutdoorTemp(°F)'][i]
    checkerOAT = 0
  else:
    checkerOAT+=1
    
  #checks for updates RAT  
  if not pd.isnull(Result['RTU_1 RATemp(°F)'][i]):
    RAT=Result['RTU_1 RATemp(°F)'][i] 
    checkerRAT=0
  else:
    checkerRAT+=1
    
  if OAT > RAT:
    print(i)

# COMMAND ----------

for i in range(loops2):
  print(Result['RTU_1 EconoStatus'][i])

# COMMAND ----------

Result

# COMMAND ----------

