-- @section DB
DROP DATABASE IF EXISTS ${hiveconf:prm_user}_landing CASCADE;
CREATE DATABASE IF NOT EXISTS ${hiveconf:prm_user}_landing LOCATION '/user/${hiveconf:prm_user}/ejercicio2/database/${hiveconf:prm_user}_landing';

-- @section TABLES
CREATE TABLE ${hiveconf:prm_user}_landing.persona
STORED AS AVRO
LOCATION '/user/${hiveconf:prm_user}/ejercicio2/database/${hiveconf:prm_user}_landing/persona'
TBLPROPERTIES (
'avro.schema.url'='hdfs:///user/${hiveconf:prm_user}/ejercicio2/schema/database/${hiveconf:prm_user}_landing/persona_avro.avsc',
'avro.output.codec'='snappy'
);

CREATE TABLE ${hiveconf:prm_user}_landing.empresa
STORED AS AVRO
LOCATION '/user/${hiveconf:prm_user}/ejercicio2/database/${hiveconf:prm_user}_landing/empresa'
TBLPROPERTIES (
'avro.schema.url'='hdfs:///user/${hiveconf:prm_user}/ejercicio2/schema/database/${hiveconf:prm_user}_landing/empresa_avro.avsc',
'avro.output.codec'='snappy'
);

CREATE TABLE ${hiveconf:prm_user}_landing.transaccion
PARTITIONED BY (FECHA STRING)
STORED AS AVRO
LOCATION '/user/${hiveconf:prm_user}/ejercicio2/database/${hiveconf:prm_user}_landing/transaccion'
TBLPROPERTIES (
'avro.schema.url'='hdfs:///user/${hiveconf:prm_user}/ejercicio2/schema/database/${hiveconf:prm_user}_landing/transaccion_avro.avsc',
'avro.output.codec'='snappy'
);

-- @section CABECERA
SET hive.exec.compress.output=true;
SET avro.output.codec=snappy;

-- @section PARTIION DINAMIC
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- @section DATA
INSERT OVERWRITE TABLE ${hiveconf:prm_user}_landing.persona
SELECT	*
FROM	${hiveconf:prm_user}_landing_temp.persona;

INSERT OVERWRITE TABLE ${hiveconf:prm_user}_landing.empresa
SELECT	*
FROM	${hiveconf:prm_user}_landing_temp.empresa;

INSERT OVERWRITE TABLE ${hiveconf:prm_user}_landing.transaccion
PARTITION(FECHA)
SELECT	*
FROM	${hiveconf:prm_user}_landing_temp.transaccion;
