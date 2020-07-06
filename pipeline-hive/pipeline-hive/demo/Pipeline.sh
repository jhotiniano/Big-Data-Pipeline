#!/bin/bash
#Creacion de Pipeline de Creacion

#Parametros
myuser=$1

#Reset
echo "Reset a Estructuras de Carpetas..."
hdfs dfs -rm -R /user/${myuser}/ejercicio2

#Creacion DB
echo "Creando estructura de carpetas..."
hdfs dfs -mkdir -p \
/user/${myuser}/ejercicio2/database/${myuser}_landing_temp \
/user/${myuser}/ejercicio2/database/${myuser}_landing \
/user/${myuser}/ejercicio2/database/${myuser}_universal \
/user/${myuser}/ejercicio2/database/${myuser}_smart
hdfs dfs -mkdir -p /user/${myuser}/ejercicio2/schema/database/${myuser}_landing

#Carga Schemas
echo "Creando schemas de tablas avro..."
hdfs dfs -put dataset/persona_avro.avsc \
/user/${myuser}/ejercicio2/schema/database/${myuser}_landing

hdfs dfs -put dataset/empresa_avro.avsc \
/user/${myuser}/ejercicio2/schema/database/${myuser}_landing

hdfs dfs -put dataset/transaccion_avro.avsc \
/user/${myuser}/ejercicio2/schema/database/${myuser}_landing


#Creacion de Procesos
echo "Proceso en Capa Landing Temp..."
beeline -u jdbc:hive2:// -f hive/${myuser}_landing_temp.hql --hiveconf prm_user=${myuser}
echo "Proceso en Capa Landing..."
beeline -u jdbc:hive2:// -f hive/${myuser}_landing.hql --hiveconf prm_user=${myuser}
echo "Proceso en Capa Universal..."
beeline -u jdbc:hive2:// -f hive/${myuser}_universal.hql --hiveconf prm_user=${myuser}
echo "Proceso en Capa Smart..."
beeline -u jdbc:hive2:// -f hive/${myuser}_smart.hql --hiveconf prm_user=${myuser}
spark2-submit spark/${myuser}_smart.py ${myuser}
