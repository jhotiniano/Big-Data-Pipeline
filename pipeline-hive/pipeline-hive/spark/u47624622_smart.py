# -*- coding: utf-8 -*-

###
 # @section Librerías
 ##

import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import udf, struct
from pyspark.sql import SparkSession
import sys

###
 # @section Parámetros
 ##

PRM_USER = sys.argv[1]

###
 # @section Tuning
 ##

spark = SparkSession.builder.\
appName("Smart").\
config("spark.master", "local[1]").\
config("spark.dynamicAllocation.maxExecutors", "10").\
config("spark.executor.cores", "2").\
config("spark.executor.memory", "500m").\
config("spark.executor.memoryOverhead", "500m").\
config("spark.driver.memory", "500m").\
config("spark.driver.memoryOverhead", "500m").\
enableHiveSupport().\
getOrCreate()

###
 # @section Funciones
 ##

###
 # Calcula el salario mensual de una persona
 #
 # @param salarioMensual {double} Salario mensual de la persona
 # @return {double}
 ##
def salarioAnual(salarioMensual):
	#Calculo del salario anual
	salarioAnual = salarioMensual*12
	#
	#Devolvemos el valor
	return salarioAnual
###
 # Calcula el salario multiplicado por la edad
 #
 # @param salario {double} Salario mensual de la persona
 # @param edad {double} Edad de la persona
 # @return {double}
 ##
def salarioPorEdad(salario, edad):
	#Calculo del salario anual
	respuesta = salario*edad
	#
	#Devolvemos el valor
	return respuesta

###
 # @section UDF
 ##

udfSalarioAnual = udf(
	salarioAnual, 
	DoubleType()
)

udfSalarioPorEdad = udf(
	(lambda parametros : salarioPorEdad(parametros[0], parametros[1])),
	DoubleType()
)

###
 # @section Programa
 ##

def main():
	###
	 # @section Lectura de datos
	 ##
	#############################
	dfPersona = spark.sql("""
		SELECT 
			* 
		FROM 
			{var:prm_user}_universal.persona
	""".\
	replace("{var:prm_user}", PRM_USER)
	)
	#dfPersona.show()
    
	#############################
	dfEmpresa = spark.sql("""
		SELECT 
			* 
		FROM 
			{var:prm_user}_universal.empresa
	""".\
	replace("{var:prm_user}", PRM_USER)
	)
	#dfEmpresa.show()
    
	#############################
	dfTransaccion = spark.sql("""
		SELECT 
			* 
		FROM 
			{var:prm_user}_universal.transaccion
	""".\
	replace("{var:prm_user}", PRM_USER)
	)
	#dfTransaccion.show()
    
    
	#
	###
	 # @section Procesamiento
	 ##
	#
	#Obtenemos el nombre de la empresa donde trabaja la persona
	dfPersonaEnriquecida = dfPersona.alias("P").join(
		dfEmpresa.alias("E"), 
		f.col("P.ID_EMPRESA") == f.col("E.ID")
	).select(
		f.col("P.ID").alias("ID_PERSONA"),
		f.col("P.NOMBRE").alias("NOMBRE_PERSONA"),
		f.col("P.EDAD").alias("EDAD_PERSONA"),
		f.col("P.SALARIO").alias("SALARIO_PERSONA"),
		f.col("E.NOMBRE").alias("TRABAJO_PERSONA")
	)
	#dfPersonaEnriquecida.show()
	#
	#Enriquecemos la transacción con los datos de la persona
	dfPersonaEnriquecidaTransaccion = dfPersonaEnriquecida.alias("P").join(
		dfTransaccion.alias("T"), 
		f.col("P.ID_PERSONA") == f.col("T.ID_PERSONA")
	).select(
		f.col("P.ID_PERSONA").alias("ID_PERSONA"),
		f.col("P.NOMBRE_PERSONA").alias("NOMBRE_PERSONA"),
		f.col("P.EDAD_PERSONA").alias("EDAD_PERSONA"),
		f.col("P.SALARIO_PERSONA").alias("SALARIO_PERSONA"),
		f.col("P.TRABAJO_PERSONA").alias("TRABAJO_PERSONA"),
		f.col("T.ID_EMPRESA").alias("ID_EMPRESA_TRANSACCION"),
		f.col("T.MONTO").alias("MONTO_TRANSACCION"),
		f.col("T.FECHA").alias("FECHA_TRANSACCION")
	)
	#dfPersonaEnriquecidaTransaccion.show()
	#
	#Enriquecemos la transacción colocando el nombre de la empresa donde se realizó la transacción
	dfReporte1 = dfPersonaEnriquecidaTransaccion.alias("P").join(
		dfEmpresa.alias("E"), 
		f.col("P.ID_EMPRESA_TRANSACCION") == f.col("E.ID")
	).select(
		f.col("P.ID_PERSONA").alias("ID_PERSONA"),
		f.col("P.NOMBRE_PERSONA").alias("NOMBRE_PERSONA"),
		f.col("P.EDAD_PERSONA").alias("EDAD_PERSONA"),
		f.col("P.SALARIO_PERSONA").alias("SALARIO_PERSONA"),
		f.col("P.TRABAJO_PERSONA").alias("TRABAJO_PERSONA"),
		f.col("P.MONTO_TRANSACCION").alias("MONTO_TRANSACCION"),
		f.col("P.FECHA_TRANSACCION").alias("FECHA_TRANSACCION"),
		f.col("E.NOMBRE").alias("EMPRESA_TRANSACCION")
	)
	#dfReporte1.show()
	#
	#Guardamos el dataframe de reporte en la tabla Hive
	dfReporte1.createOrReplaceTempView("dfReporte1")
	#
	#Almacenamos el resultado en Hive
	spark.sql("""
		INSERT INTO {var:prm_user}_smart.reporte_1
			SELECT
				*
			FROM 
				dfReporte1 T
	""".\
	replace("{var:prm_user}", PRM_USER)
	)
	#
	#Verificamos que el reporte se haya insertado correctamente
	#spark.sql("""
	#	SELECT 
	#		* 
	#	FROM 
	#		{var:prm_user}_smart.reporte_1 
	#	LIMIT 10
	#""".\
	#replace("{var:prm_user}", PRM_USER)
	#).show()


if __name__ == "__main__":
    main()
