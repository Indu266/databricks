-- Databricks notebook source
CREATE LIVE TABLE drivers
COMMENT "The drivers data ingested from josn file."
TBLPROPERTIES ("myCompanyPipeline.quality" = "mapping")
AS SELECT * FROM json.`dbfs:/FileStore/tables/formula1/drivers.json`

-- COMMAND ----------

CREATE  LIVE TABLE pitstops
COMMENT"The raw pitstops data from json."
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze")
AS
SELECT * FROM json.`dbfs:/FileStore/tables/formula1/pit_stops.json`

-- COMMAND ----------

CREATE LIVE TABLE cleaned_data
USING DELTA
PARTITIONED BY (raceId)
COMMENT "The cleaned data partitioned by raceId."
TBLPROPERTIES ("myCompanyPipeline.quality" = "silver")
AS
SELECT f.driverId, f.duration, f.lap, d.driverRef, d.name
FROM LIVE.pitstops f
LEFT JOIN LIVE.drivers d
ON f.driverId = d.driverId

-- COMMAND ----------

CREATE LIVE TABLE 17thlap_in_pitstops
COMMENT "17thlap in pitstops."
TBLPROPERTIES ("myCompanyPipeline.quality" = "gold")
AS
SELECT driverId,duration,lap,milliseconds,raceId,stop as no_of_stops ,time as time_taken
FROM (
  SELECT driverId,duration,lap,milliseconds,raceId
  FROM LIVE.cleaned_data 
  WHERE lap =17
  )
GROUP BY driverId,raceId

-- COMMAND ----------

CREATE LIVE TABLE 40thlap_in_pitstops
COMMENT "40thlap in pitstops."
TBLPROPERTIES ("myCompanyPipeline.quality" = "gold")
AS
SELECT driverId,duration,lap,milliseconds,raceId,stop as no_of_stops ,time as time_taken
FROM (
  SELECT driverId,duration,lap,milliseconds,raceId
  FROM LIVE.cleaned_data 
  WHERE lap =40
  )
GROUP BY driverId,raceId
