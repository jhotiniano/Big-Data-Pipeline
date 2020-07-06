-- @section DB
DROP DATABASE IF EXISTS ${hiveconf:prm_user}_universal CASCADE;
CREATE DATABASE IF NOT EXISTS ${hiveconf:prm_user}_universal LOCATION '/user/${hiveconf:prm_user}/ejercicio2/database/${hiveconf:prm_user}_universal';

-- @section TABLES
CREATE TABLE ${hiveconf:prm_user}_universal.persona(
ID 				STRING,
NOMBRE 			STRING,
TELEFONO 		STRING,
CORREO 			STRING,
FECHA_INGRESO 	STRING,
EDAD 			INT,
SALARIO 		DOUBLE,
ID_EMPRESA 		STRING
)
STORED AS PARQUET
LOCATION '/user/${hiveconf:prm_user}/ejercicio2/database/${hiveconf:prm_user}_universal/persona'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE TABLE ${hiveconf:prm_user}_universal.empresa(
ID 				STRING,
NOMBRE 			STRING
)
STORED AS PARQUET
LOCATION '/user/${hiveconf:prm_user}/ejercicio2/database/${hiveconf:prm_user}_universal/empresa'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE TABLE ${hiveconf:prm_user}_universal.transaccion(
ID_PERSONA 		STRING,
ID_EMPRESA 		STRING,
MONTO 			DOUBLE
)
PARTITIONED BY (FECHA STRING)
STORED AS PARQUET
LOCATION '/user/${hiveconf:prm_user}/ejercicio2/database/${hiveconf:prm_user}_universal/transaccion'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE TABLE ${hiveconf:prm_user}_universal.transaccion_enriquecida(
ID_PERSONA				INT,
NOMBRE_PERSONA			STRING,
EDAD_PERSONA			INT,
SALARIO_PERSONA			DOUBLE,
TRABAJO_PERSONA			STRING,
MONTO_TRANSACCION		DOUBLE,
EMPRESA_TRANSACCION		STRING
)
PARTITIONED BY (FECHA_TRANSACCION STRING)
STORED AS PARQUET
LOCATION '/user/${hiveconf:prm_user}/ejercicio2/database/${hiveconf:prm_user}_universal/transaccion_enriquecida'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- @section TUNNING
--SET mapreduce.job.maps=8;
--SET mapreduce.input.fileinputformat.split.maxsize = 128000000;
--SET mapreduce.input.fileinputformat.split.minsize = 128000000;
--SET mapreduce.map.cpu.vcores=2;
--SET mapreduce.map.memory.mb=128;
--SET mapreduce.job.reduces=8;
--SET mapreduce.reduce.cpu.vcores=2;
--SET mapreduce.reduce.memory.mb=128;

-- @section COMPRESION SNAPPY
SET hive.exec.compress.output=true;
SET parquet.compression=SNAPPY;

-- @section PARTIION DINAMIC
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- @section DATA
INSERT OVERWRITE TABLE ${hiveconf:prm_user}_universal.persona
SELECT	*
FROM	${hiveconf:prm_user}_landing.persona
WHERE	ID != 'ID';

INSERT OVERWRITE TABLE ${hiveconf:prm_user}_universal.empresa
SELECT	*
FROM	${hiveconf:prm_user}_landing.empresa
WHERE	ID != 'ID';

INSERT OVERWRITE TABLE ${hiveconf:prm_user}_universal.transaccion
PARTITION(FECHA)
SELECT	*
FROM	${hiveconf:prm_user}_landing.transaccion
WHERE	ID_PERSONA != 'ID_PERSONA';

INSERT OVERWRITE TABLE ${hiveconf:prm_user}_universal.transaccion_enriquecida
PARTITION(FECHA_TRANSACCION)
SELECT	a.id_persona,
		b.nombre,
		b.edad,
		b.salario,
		d.nombre,
		a.monto,
		c.nombre,
		a.fecha AS FECHA_TRANSACCION
FROM	${hiveconf:prm_user}_universal.transaccion a
		INNER JOIN ${hiveconf:prm_user}_universal.persona b ON a.ID_PERSONA=b.ID
		INNER JOIN ${hiveconf:prm_user}_universal.empresa c ON a.ID_EMPRESA=c.ID
		INNER JOIN ${hiveconf:prm_user}_universal.empresa d ON b.ID_EMPRESA=d.ID;
