#!/bin/bash

export YEAR=${1}
export MONTH=${2}
export FILE_PATH=${3}

echo $YEAR$MONTH.csv
sed 's/,$//g' /tmp/$YEAR$MONTH.csv | sed 's/"//g' > /tmp/temp_$YEAR$MONTH.csv
mv /tmp/temp_$YEAR$MONTH.csv /tmp/$YEAR$MONTH.csv
