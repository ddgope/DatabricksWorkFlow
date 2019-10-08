# Databricks notebook source
dbutils.library.installPyPI('yfinance')
dbutils.library.installPyPI('pandas_datareader')
dbutils.library.restartPython()
dbutils.library.list()

# COMMAND ----------

# DBTITLE 1,Full Load Finance Data
import pyspark.sql.functions as f
from YahooFinanceLib import financedata as finD

# COMMAND ----------

# DBTITLE 1,Get Notebook input parameters
dbutils.widgets.text("tickerName", "","")
dbutils.widgets.get("tickerName")
tickerName = getArgument("tickerName")

dbutils.widgets.text("start_date", "","")
dbutils.widgets.get("start_date")
start_date = getArgument("start_date")

dbutils.widgets.text("end_date", "","")
dbutils.widgets.get("end_date")
end_date = getArgument("end_date")

# COMMAND ----------

# DBTITLE 1,Use Notebook paremters to get load time series data
#tickerName='MSFT'# AAPL GOOGL'
#start_date='2019-08-01'
#end_date='2019-08-31'
dailyClose= spark.createDataFrame(finD.getFinanceData(tickerName,start_date,end_date))
## To create a Spark Dataframe from pandas and infer the schema, a single column is returned when the API call is unable to populate result Can't infer from an empty dataframe
if len(dailyClose.columns) > 1:  
  data_loaded = True

# COMMAND ----------

# DBTITLE 1,Create partitions on Year and Month Columns
if data_loaded == True:
  
  dailyClose = dailyClose.withColumn('year',f.year(dailyClose.date))
  dailyClose = dailyClose.withColumn('month',f.month(dailyClose.date))
  
else:
    print("No Data Loaded")
    dbutils.notebook.exit(0)


# COMMAND ----------

# DBTITLE 1,Save results to table
dailyClose.write.insertInto('dailyClose')

# COMMAND ----------

# DBTITLE 1,Exit Notebook and return number of records in dataset
dbutils.notebook.exit(dailyClose.count())