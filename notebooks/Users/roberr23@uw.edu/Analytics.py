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
all_csv_files

# COMMAND ----------

pd.read_csv("/dbfs/FileStore/tables/RTU_1_BldgStatPress_2019-343cc.csv")


# COMMAND ----------

import numpy as np
varlist=[]

for i in range(int(loops)):
    globals() ['df'+str(i)] = pd.read_csv(all_csv_files[i])
    globals() ['df'+str(i)].set_index('Timestamp', inplace=True)
    if i == 0:
      result = df0
    else:
      result = result.merge(globals() ['df'+str(i)], on='Timestamp',how='left')
      
result

# COMMAND ----------


import pandas as pd
import numpy as np
 

# generating some test data
timestamp = [1440540000, 1450540000]
df1 = pd.DataFrame(
    {'timestamp': timestamp, 'a': ['val_a', 'val2_a'], 'b': ['val_b', 'val2_b'], 'c': ['val_c', 'val2_c']})
print(df1)
 

# building a different index
timestamp = timestamp * np.random.randn(abs(1))
df2 = pd.DataFrame(
    {'timestamp': timestamp, 'd': ['val_d', 'val2_d'], 'e': ['val_e', 'val2_e'], 'f': ['val_f', 'val2_f'],
     'g': ['val_g', 'val2_g']}, index=timestamp)
print(df2)
 

# keeping a value in common with the first index
timestamp = [1440540000, 1450560000]
df3 = pd.DataFrame({'timestamp': timestamp, 'h': ['val_h', 'val2_h'], 'i': ['val_i', 'val2_i']}, index=timestamp)
print(df3)

 
# Setting the timestamp as the index
df1.set_index('timestamp', inplace=True)
df2.set_index('timestamp', inplace=True)
df3.set_index('timestamp', inplace=True)
 
# You can convert timestamps to dates but it's not mandatory I think
df1.index = pd.to_datetime(df1.index, unit='s')
df2.index = pd.to_datetime(df2.index, unit='s')
df3.index = pd.to_datetime(df3.index, unit='s')
 
# Just perform a join and that's it
#result=df1.merge(df2, on='timestamp',how='left').merge(df3, on='timestamp',how='left')
result = df1.join(df2, how='outer').join(df3, how='outer')
result

# COMMAND ----------

ax = plt.gca()

result.plot(kind='line',x='Timestamp',y='RTU_1 OutdoorTemp(°F)',ax=ax)
#RATemp2019.plot(kind='line',x='Timestamp',y='RTU_1 RATemp(°F)', color='red', ax=ax)
#plt.xticks(rotation='vertical')

plt.show()
#here we're looking at retrun air vs outdoor temp
#anywhere where OutDoorTemp>RATemp we expect the 
#economizer to be active

# COMMAND ----------

result['Timestamp']

# COMMAND ----------

print(all_csv_files)

# COMMAND ----------

dbfs ls

# COMMAND ----------

f

# COMMAND ----------

#this line is changed