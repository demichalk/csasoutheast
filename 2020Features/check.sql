-- Databricks notebook source
create database if not exists demo2020;

-- COMMAND ----------

drop table if exists demo2020.people;

-- COMMAND ----------

-- MAGIC %fs rm -r abfss://lake@fieldengdeveastus2adls.dfs.core.windows.net/bronze/demo2020/people

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo2020.people
(
  First string NOT NULL,
  Last string NOT NULL,
  Age integer
)
USING delta
LOCATION 'abfss://lake@fieldengdeveastus2adls.dfs.core.windows.net/bronze/demo2020/people'

-- COMMAND ----------

INSERT INTO demo2020.people VALUES ('dale','michalk',49)

-- COMMAND ----------

INSERT INTO demo2020.people VALUES (null,'michalk',20)

-- COMMAND ----------

INSERT INTO demo2020.people VALUES ('satya','nadella',53)

-- COMMAND ----------

alter table demo2020.people add constraint agefilter CHECK (Age < 50);

-- COMMAND ----------

delete from demo2020.people where age >= 50

-- COMMAND ----------

alter table demo2020.people add constraint agefilter CHECK (Age < 50);

-- COMMAND ----------

describe detail demo2020.people;

-- COMMAND ----------

INSERT INTO demo2020.people VALUES ('ali','ghodsi',37)

-- COMMAND ----------

INSERT INTO demo2020.people VALUES ('bill','gates',65)

-- COMMAND ----------

alter table demo2020.people drop constraint agefilter;

-- COMMAND ----------

INSERT INTO demo2020.people VALUES ('bill','gates',65)

-- COMMAND ----------

