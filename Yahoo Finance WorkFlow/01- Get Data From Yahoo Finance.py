# Databricks notebook source
dbutils.library.installPyPI('yfinance')
dbutils.library.installPyPI('pandas_datareader')
#dbutils.library.installPyPI('fastparquet')
dbutils.library.restartPython()
dbutils.library.list()

# COMMAND ----------

import yfinance as data
import matplotlib.pyplot as plt
import pandas as pd
import datetime

start_date = '2014-08-31'
end_date = '2019-08-31'

from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override() # <== that's all it takes :-) #data = yf.download("SPY AAPL MSFT", start="2018-01-01", end="2018-04-30",group_by="ticker") 
# download dataframe and Use pandas_reader.data.DataReader to load the desired data. As simple as that.
appleData = pdr.get_data_yahoo("AAPL",start_date, end_date,group_by="ticker") 
#spyData = pdr.get_data_yahoo("SPY",start_date, end_date,group_by="ticker") 

# COMMAND ----------

appleData.head(5)

# COMMAND ----------

#appleData.shape[1]
#appleData.shape[1]
#appleData.iloc[0][1]
appleData.head(5)
appleData.describe
#Reindex - to get the Date column
appleDataFinal=appleData.rename_axis('date').reset_index()
#appleData.reset_index(level='Date',inplace=True,drop=False)
appleDataFinal.head(5)
#data.to_parquet('apple.parquet', engine='fastparquet',compression='snappy')



# COMMAND ----------

# DBTITLE 1,Apple Stock Data Frame
#Converted to PySpark Data Frame into Apple Data Frame
appleDF = spark.createDataFrame(appleDataFinal)
appleDF.printSchema()
appleDF.registerTempTable("Apple")
type(appleDF)

#Calculate and display the average closing price per month for Apple 
avgClsPriceByMonthApple = spark.sql("""SELECT year(Date) as yr, month(Date) as mo,round(avg(`Adj Close`),3) as appleavgclose 
from Apple 
group By year(Date), month(Date) 
order by year(Date) desc,month(Date) desc""")
#avgClsPriceByMonthApple.show(5)
display(avgClsPriceByMonthApple)

#Compute the average closing price per year for Apple
avgClsPriceByYearApple = spark.sql("""SELECT year(Date) as yr,round(avg(`Adj Close`),3) as appleavgclose 
from Apple 
group By year(Date)
order by year(Date) desc""")
#avgClsPriceByYearApple.show(5)
display(avgClsPriceByYearApple)

# COMMAND ----------

# DBTITLE 1,S&P 500 ETF Trust (SPY) Data Frame
#Converted to PySpark Data Frame into Apple Data Frame
spyDF = spark.createDataFrame(spyDataFinal)
spyDF.printSchema()
spyDF.registerTempTable("Spy")
type(spyDF)

#Calculate and display the average closing price per month for Apple 
avgClsPriceByMonthSpy = spark.sql("""SELECT year(Date) as yr, month(Date) as mo,round(avg(`Adj Close`),3) as appleavgclose 
from Spy 
group By year(Date), month(Date) 
order by year(Date) desc,month(Date) desc""")
avgClsPriceByMonthSpy.show(5)

#Compute the average closing price per year for Apple
avgClsPriceByYearSpy = spark.sql("""SELECT year(Date) as yr,round(avg(`Adj Close`),3) as appleavgclose 
from Spy 
group By year(Date)
order by year(Date) desc""")
#avgClsPriceByYearSpy.show(5)
display(avgClsPriceByYearSpy)

# COMMAND ----------

#List  when  the closing price for SPY AND Apple went up or down by more than 2 dollars
compareAppleSpy = spark.sql("""SELECT Spy.date, round(abs(Spy.Close - Spy.Open),3) as SpyDif, 
Apple.date,round(abs(Apple.Close - Apple.Open),3) as AppleDif 
FROM Spy 
join Apple 
on Spy.date = Apple.date 
WHERE (abs(Spy.Close - Spy.Open) > 2 and abs(Apple.Close - Apple.Open) > 2)""")
#compareAppleSpy.show(5)
display(compareAppleSpy)

# COMMAND ----------

#max, min closing price for SPY and Apple by Year
maxminCloseAppleSpy = spark.sql("""SELECT year(Spy.date) as yr, max(Spy.`Adj Close`), 
  min(Spy.`Adj Close`), 
  max(Apple.`Adj Close`), min(Apple.`Adj Close`) 
  FROM Spy 
  join Apple on Spy.date = Apple.date  
  group By year(Spy.date)""")
#maxminCloseAppleSpy.show(5)
display(maxminCloseAppleSpy)
