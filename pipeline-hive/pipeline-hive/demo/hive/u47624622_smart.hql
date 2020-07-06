--
-- @section Parámetros
--

-- Imprimimos los parámetros
SELECT	T.PARAM_NAME, T.PARAM_VALUE 
FROM	(
		-- Nombre de usuario
		SELECT 	"prm_user" PARAM_NAME,
				"${hiveconf:prm_user}" PARAM_VALUE
		) T;

--
-- @section Programa
--

-- Creación de base de datos
CREATE DATABASE IF NOT EXISTS ${hiveconf:prm_user}_SMART LOCATION '/user/${hiveconf:prm_user}/ejercicio2/database/${hiveconf:prm_user}_smart';

-- Creamos la tabla
DROP TABLE IF EXISTS ${hiveconf:prm_user}_smart.reporte_1;
CREATE TABLE ${hiveconf:prm_user}_smart.reporte_1(
ID_PERSONA INT,
NOMBRE_PERSONA STRING,
EDAD_PERSONA INT,
SALARIO_PERSONA DOUBLE,
TRABAJO_PERSONA STRING,
MONTO_TRANSACCION DOUBLE,
FECHA_TRANSACCION STRING,
EMPRESA_TRANSACCION STRING
)
STORED AS PARQUET
LOCATION '/user/${hiveconf:prm_user}/ejercicio2/database/${hiveconf:prm_user}_smart/reporte_1'
TBLPROPERTIES ("parquet.compression"="SNAPPY");
