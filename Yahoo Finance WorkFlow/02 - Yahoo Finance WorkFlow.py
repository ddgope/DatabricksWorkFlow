# Databricks notebook source
# DBTITLE 1,Finance Data Workflow
from datetime import datetime,timedelta
from multiprocessing.pool import ThreadPool
import pyspark.sql.functions as f

#set number of concurrent jobs in pool - this API will error more frequently with the number of running threads, be warned.
pool = ThreadPool(10)

# COMMAND ----------

# DBTITLE 1,Write Finance Data to Azure SQL DB
def writeToSQLDB():
  jdbcUsername = "xxxxx"  ##SQL USER
  jdbcPassword = "xxxxx"  ##SQL USER Password
  jdbcHostname = "xxxxx.database.windows.net"  ##Azure SQL DB #####.database.windows.ney
  jdbcPort = 1433
  jdbcDatabase ="xxxxx"  ##Azure SQL DB Name


  jdbc_url =r"jdbc:sqlserver://%s:%s;database=%s;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=60;" % (jdbcHostname,jdbcPort,jdbcDatabase)

  properties = {
    "user": jdbcUsername,
    "password": jdbcPassword
  }

  ## Write the Databricks table ('NoaaData') to Azure SQL DB table ('NoaaData') - will create if not exists
  spark.table("DailyClose").write.jdbc(jdbc_url, "DailyClose", "overwrite", properties)

# COMMAND ----------

# DBTITLE 1,Load Date Dimension
import pandas as pd
def create_date_table(start='2000-01-01', end='2037-12-31'):
   df = pd.DataFrame({"date": pd.date_range(start, end)})
   df["week_day"] = df.date.dt.weekday_name
   df["day"] = df.date.dt.day
   df["month"] = df.date.dt.day
   df["week"] = df.date.dt.weekofyear
   df["quarter"] = df.date.dt.quarter
   df["year"] = df.date.dt.year
   df.insert(0, 'cobdate', (df.year.astype(str) + df.month.astype(str).str.zfill(2) + df.day.astype(str).str.zfill(2)).astype(int))
   return df

dateDim=create_date_table()
dateDim.head(5)

# COMMAND ----------

# DBTITLE 1,Load Company Dimension
import pandas as pd  
    
# List1  
Name = ['Apple', 'Microsoft', 'Google', 'Facebook']  
    
# List2  
Id = [1, 2, 3, 4]  

#List 3
Ticker=['AAPL','MSFT','GOOGL','FB']
    
# get the list of tuples from two lists.  
# and merge them by using zip().  
list_of_tuples = list(zip(Id, Name, Ticker))  
    
# Assign data to tuples.  
list_of_tuples
  
  
# Converting lists of tuples into  
# pandas Dataframe.  
df = pd.DataFrame(list_of_tuples, columns = ['Id','Name','Ticker'])  
     
# Print data.  
df  

# COMMAND ----------

# DBTITLE 1,Prepard for load of historical
# MAGIC %sql drop table if exists DailyClose;

# COMMAND ----------

# MAGIC %fs rm -r /delta/temp

# COMMAND ----------

# MAGIC %fs rm -r /delta/DailyClose

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE table if not exists  DailyClose
# MAGIC (date timestamp,
# MAGIC Adj_Close double,
# MAGIC Close double,
# MAGIC High double,
# MAGIC Low double,
# MAGIC Open double,
# MAGIC Volume int,
# MAGIC tickerName string,
# MAGIC cobdate string,
# MAGIC year int,
# MAGIC month int)
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (year, month)
# MAGIC LOCATION '/delta/temp'

# COMMAND ----------

# DBTITLE 1,Set Start and End dates for loading data - each month will run a new Notebook job
tickerName='AAPL'# AAPL GOOGL'
start_date='2019-01-01'
end_date='2019-09-30'

# COMMAND ----------

## Create monthly nested array [FOM, EOM]
import pandas as pd

datelist = pd.date_range(start_date, end_date, freq='M').tolist()

datelist = [[dt.strftime('%Y-%m-01'),dt.strftime('%Y-%m-%d')]  for dt in datelist]

print(datelist)

# COMMAND ----------

pool.map(lambda arg: dbutils.notebook.run("/Users/durgadas.g@hcl.com/03 - Yahoo Finance Historic Load",                                          
                                         timeout_seconds=300,
                                         arguments = {"tickerName": tickerName,"start_date":arg[0], "end_date":arg[1]}), datelist)

# COMMAND ----------

dsCompleteData = sqlContext.sql('select * from DailyClose')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(1) FROM DailyClose;

# COMMAND ----------

# DBTITLE 1,Final Fact Table Creation
dsCompleteData = dsCompleteData.withColumn('date',f.to_date(dsCompleteData.date))
dsCompleteData = dsCompleteData.withColumn('year',f.year(dsCompleteData.date))
dsCompleteData = dsCompleteData.withColumn('month',f.month(dsCompleteData.date))

# COMMAND ----------

dsCompleteData.write.insertInto('DailyClose')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM DailyClose limit 10;

# COMMAND ----------

# DBTITLE 1,Connect Power BI to Azure SQL DB, follow the steps to create the database then run below
writeToSQLDB()