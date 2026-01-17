# Databricks notebook source
dimdatedf= spark.table("onlinecommerce.bronze.fiscalperiod")
display(dimdatedf)

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd
import dateutil

# COMMAND ----------

# DBTITLE 1,Cell 3
import datetime

start_date = datetime.date(2018,1,1)
end_date = start_date + dateutil.relativedelta.relativedelta(years=8,month=12,day=31)


start_date = datetime.datetime.strptime(
    f"{start_date}", "%Y-%m-%d"
)
end_date = datetime.datetime.strptime(
    f"{end_date}", "%Y-%m-%d"
)
print(start_date)
print(end_date)

# COMMAND ----------

datepddf = pd.date_range(start_date,end_date, freq='D').to_frame(name='Date')
datedf=spark.createDataFrame(datepddf)
display(datedf)

# COMMAND ----------

# DBTITLE 1,Untitled
joindf = (
    datedf.join(
        dimdatedf.filter(dimdatedf.RecordId.isNotNull()),
         (datedf.Date >= dimdatedf.FiscalStartDate)
        & (datedf.Date <= dimdatedf.FiscalEndDate),
        "left",
    ))
display(joindf)

# COMMAND ----------

datedimdf = joindf.select(
    "Date",
    F.date_format(F.col("Date"), "yyyyMMdd").cast("int").alias("DateId"),
    F.year(F.col("Date")).alias("Year"),
    F.month(F.col("Date")).alias("Month"),
    F.date_format(F.col("Date"), "MMM").cast("string").alias("MonthName"),
    F.dayofmonth(F.col("Date")).alias("Day"),
    F.date_format(F.col("Date"), "E").cast("string").alias("DayName"),
    F.quarter(F.col("Date")).alias("Quarter"),
    F.col("FiscalPeriodName").alias("FiscalPeriodName"),    
    "FiscalStartDate",
    "FiscalEndDate",
    "FiscalMonth",
    "FiscalYearStart",
    "FiscalYearEnd",
    "FiscalQuarter",
    "FiscalQuarterStart",
    "FiscalQuarterEnd",
    F.concat(F.lit("FY"),"FiscalYear").alias("FiscalYear"),
    F.current_timestamp().alias("UpdatedDateTime"),
    F.xxhash64("DateId").alias("DateKey")
)
display(datedimdf)

# COMMAND ----------

datedimdf.write.format("delta").mode("overwrite").saveAsTable("onlinecommerce.silver.dimdate")
