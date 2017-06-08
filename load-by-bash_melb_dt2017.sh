#!/bin/bash

BDL_VERSION='1.1.0'
echo ""
#echo "Bash version ${BASH_VERSION}..."
echo "--- Batch Data Loader version ${BDL_VERSION} ---"

# Assign user and password and database args
username="${1}"
password="${2}"
database="${3}"

 
# List the parameter values passed.
echo ""
#echo "Username:  " ${username}
#echo "Password:  " ${password}
#echo "Database:  " ${database}
echo ""

for i in {1..50}
  do 
     #load each of the individual files alone
     # _name - local unix home_dir_name
     time mysql -u${username} -p${password} -hlocalhost -D${database} -e "LOAD DATA LOCAL INFILE '/home/_name/temp_data/MelbDatathon2017/Transactions/patients_"$i".txt' INTO TABLE transactions FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES"
     
     echo "...Success:loaded patients_"$i".txt'"
     
 done
