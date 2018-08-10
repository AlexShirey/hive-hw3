#/user/bin/bash

if [[ $# -ne 3 ]]; then
	echo "error: illegal number of parameters (HQL_FILE, CLIENT, ENGINE)"
	exit -1
fi

HQL_FILE=$(echo $1 | tr -d '\r')
client=$2
engine=$3

if [[ ! -f "${HQL_FILE}" ]]; then
	echo "error: "${HQL_FILE}" is not a file or doesn't exists"
	exit -1
fi

echo "starting $client (engine is $engine) to execute HQL from $HQL_FILE..."

if [[ $client == "hive" ]]; then
	hive --hivevar db="${DB}" --hivevar creator="${CREATOR}" --hivevar date="${DATE}" \
	     --hivevar city_data_dir=$CDD --hivevar bid_data_dir=$BDD --hivevar udf_lib=$UDF_LIB \
	     --hiveconf hive.execution.engine=$engine -f $HQL_FILE \
	     --hiveconf hive.variable.substitute=true
else
	beeline -u jdbc:hive2://localhost:10000 --hiveconf hive.execution.engine=$engine -f $HQL_FILE \
		--hivevar db="${DB}" --hivevar creator="${CREATOR}" --hivevar date="${DATE}" \
	    --hivevar city_data_dir=$CDD --hivevar bid_data_dir=$BDD --hivevar udf_lib=$UDF_LIB 
fi

echo "...execution of $HQL_FILE finished."