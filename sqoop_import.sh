#!/bin/bash
# Import sales_data table into Hive
sqoop import \
  --connect jdbc:mysql://database-1.c5w644s0i0kh.ap-south-1.rds.amazonaws.com/dmart \
  --username admin \
  --password Karthankar \
  --table sales_data \
  --hive-import \
  --hive-database dmart \
  --hive-table sales_data \
  --create-hive-table \
  --num-mappers 1
# Import store_data table into Hive
sqoop import \
  --connect jdbc:mysql://database-1.c5w644s0i0kh.ap-south-1.rds.amazonaws.com/dmart \
  --username admin \
  --password Karthankar \
  --table store_data \
  --hive-import \
  --hive-database dmart \
  --hive-table store_data \
  --create-hive-table \
  --num-mappers 1
