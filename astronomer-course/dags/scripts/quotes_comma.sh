#!/bin/bash

export YEAR=${1}
export MONTH=${2}
export FILE_PATH=${3}

echo $YEAR$month.csv
sed 's/,$//g' $YEAR$month.csv | sed 's/"//g' > $FILE_PATH/tmp
mv $FILE_PATH/tmp $FILE_PATH/$YEAR$month.csv
