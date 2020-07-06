#!/bin/bash
rm data*.csv
file=$1
awk -F ";" '$3<1' $file>data1.csv
awk -F ";" '$3>20 && $3<26' $file>data2.csv
awk -F ";" '$3>25 && $3<31' $file>data3.csv
awk -F ";" '$3>31' $file>data4.csv
