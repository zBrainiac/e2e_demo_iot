DROP VIEW IF EXISTS view_sensor_10;
DROP VIEW IF EXISTS view_sensor_88;
DROP MATERIALIZED VIEW IF EXISTS view_matwerialized_join_iot_geo;
DROP TABLE IF EXISTS table_managed_geoloc;
DROP TABLE IF EXISTS table_managed_iot;
DROP TABLE IF EXISTS table_ext_join;
DROP TABLE IF EXISTS table_ext_iot;
DROP TABLE IF EXISTS table_ext_geoloc;
DROP DATABASE IF EXISTS iot;


CREATE DATABASE if not exists iot;

USE iot;

CREATE EXTERNAL TABLE if not exists iot.table_ext_geoloc (
sensor_id INT
,city STRING
,lat DOUBLE
, lon DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LOCATION "s3a://demo-aws-2/user/mdaeppen/data_geo"
tblproperties ("skip.header.line.count"="1", "external"="true");

SELECT * FROM iot.table_ext_geoloc
LIMIT 3;

CREATE TABLE if not exists iot.table_managed_geoloc
STORED AS orc
as select * from iot.table_ext_geoloc;

SELECT * FROM iot.table_managed_geoloc
LIMIT 3;

CREATE EXTERNAL TABLE if not exists iot.table_ext_iot (
sensor_id INT
,field_2 INT
,field_3 INT
,field_4 INT
,field_5 DOUBLE
,field_6 INT
,field_7 DOUBLE
,field_8 INT
,field_9 DOUBLE
,field_10 INT
,field_11 DOUBLE
,field_12 INT
,field_13 INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
LOCATION "s3a://demo-aws-2/user/mdaeppen/data_iot";

SELECT * FROM iot.table_ext_iot
LIMIT 3;

CREATE TABLE if not exists iot.table_managed_iot
STORED AS orc
as select * from iot.table_ext_iot;

SELECT * FROM iot.table_managed_iot
LIMIT 3;

CREATE VIEW if not exists iot.view_sensor_10
as select * from iot.table_managed_iot where sensor_id = 10 ;

CREATE VIEW if not exists iot.view_sensor_88
as select * from iot.table_managed_iot where sensor_id = 88 ;


SELECT iot.sensor_id, geoloc.city, geoloc.lat, geoloc.lon FROM iot.table_managed_iot iot, iot.table_managed_geoloc geoloc
WHERE iot.sensor_id = geoloc.sensor_id
Limit 3;

SELECT iot.sensor_id, geoloc.city, geoloc.lat, geoloc.lon ,count(*) as counter FROM iot.table_managed_iot iot , iot.table_managed_geoloc geoloc
WHERE iot.sensor_id = geoloc.sensor_id
GROUP BY iot.sensor_id, geoloc.city, geoloc.lat, geoloc.lon
ORDER BY counter DESC;


CREATE MATERIALIZED VIEW IF NOT EXISTS iot.view_matwerialized_join_iot_geo
STORED AS ORC
AS SELECT iot.sensor_id, geoloc.city, geoloc.lat, geoloc.lon  FROM iot.table_managed_iot iot, iot.table_managed_geoloc geoloc WHERE iot.sensor_id = geoloc.sensor_id ;

SELECT * FROM iot.view_matwerialized_join_iot_geo;

SELECT sensor_id, city, lat, lon ,count(*) as counter FROM iot.view_matwerialized_join_iot_geo
GROUP BY sensor_id, city, lat, lon
ORDER BY counter DESC;
