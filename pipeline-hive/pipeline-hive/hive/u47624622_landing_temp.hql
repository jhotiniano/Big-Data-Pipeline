-- @section DB
DROP DATABASE IF EXISTS ${hiveconf:prm_user}_landing_temp CASCADE;
CREATE DATABASE IF NOT EXISTS ${hiveconf:prm_user}_landing_temp LOCATION '/user/${hiveconf:prm_user}/ejercicio2/database/${hiveconf:prm_user}_landing_temp';

-- @section TABLES
CREATE TABLE ${hiveconf:prm_user}_landing_temp.persona(
ID STRING,
NOMBRE STRING,
TELEFONO STRING,
CORREO STRING,
FECHA_INGRESO STRING,
EDAD STRING,
SALARIO STRING,
ID_EMPRESA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/${hiveconf:prm_user}/ejercicio2/database/${hiveconf:prm_user}_landing_temp/persona';

CREATE TABLE ${hiveconf:prm_user}_landing_temp.empresa(
ID STRING,
NOMBRE STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/${hiveconf:prm_user}/ejercicio2/database/${hiveconf:prm_user}_landing_temp/empresa';

CREATE TABLE ${hiveconf:prm_user}_landing_temp.transaccion(
ID_PERSONA	STRING,
ID_EMPRESA	STRING,
MONTO		STRING,
FECHA		STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/${hiveconf:prm_user}/ejercicio2/database/${hiveconf:prm_user}_landing_temp/transaccion';

-- @section DATA
LOAD DATA LOCAL INPATH 'dataset/persona.data' INTO TABLE ${hiveconf:prm_user}_landing_temp.persona;
LOAD DATA LOCAL INPATH 'dataset/empresa.data' INTO TABLE ${hiveconf:prm_user}_landing_temp.empresa;
LOAD DATA LOCAL INPATH 'dataset/transacciones.data' INTO TABLE ${hiveconf:prm_user}_landing_temp.transaccion;

