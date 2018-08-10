#/user/bin/bash

if [[ $# -ne 2 ]]; then
        echo "illegal number of parameters (CLIENT, ENGINE)"
	exit -1
fi
                                                                                        
client=$1
engine=$2
config_file=./etc/app.config
execute_file=./etc/execute.sh
                                                                                     
if ! [[ $client == "hive" || $client == "beeline" ]]; then
	echo "client should be hive or beeline"
	exit -1
fi

if ! [[ $engine == "mr" || $engine == "tez" ]]; then
	echo "engine should be mr or tez"
	exit -1
fi

if [[ ! -e $config_file ]]; then
	echo "missing app.config file in path ./etc/app.config"
	exit -1
fi

if [[ ! -e $execute_file ]]; then
	echo "missing execute.sh file in path ./etc/execute.sh"
	exit -1
fi

export CREATOR="$(awk '/creator/{getline; print}' $config_file)"
export DB="$(awk '/database/{getline; print}' $config_file)"
CDD="$(awk '/city_data_dir/{getline; print}' $config_file)"
BDD="$(awk '/bid_data_dir/{getline; print}' $config_file)"
UDF_LIB="$(awk '/udf_lib/{getline; print}' $config_file)"
export DATE=$(date)

export CDD=$(echo $CDD | tr -d '\r')
export BDD=$(echo $BDD | tr -d '\r')
export UDF_LIB=$(echo $UDF_LIB | tr -d '\r')


if [[ -z $CREATOR || -z $DB || -z $CDD || -z $BDD || -z $UDF_LIB ]]; then
	echo "some of the property is empty, check $config_file"
	exit -1
fi

execute ()
{
bash $execute_file "${HQL}" $client $engine
}

select_option () 
{
echo "please, select what to do:"
echo "0 - init tables and load data"
echo "1 - create custom UDF function"
echo "2 - find most popular device, browser, OS for each city using custom UDF."
echo "other - other hql to execute"
echo "quit - quit"

read OPTION

case $OPTION in
        0) HQL="$(awk '/00_hql/{getline; print}' $config_file)" ;;			
        1) HQL="$(awk '/01_hql/{getline; print}' $config_file)" ;;
		2) HQL="$(awk '/02_hql/{getline; print}' $config_file)" ;;
        "other") echo "please, specify the path to HQL file:"
                read HQL ;;
        "quit") exit -1 ;;
        *) echo "error: wrong option!"
           select_option
        esac
}

while true
	do
		select_option
        execute
    done