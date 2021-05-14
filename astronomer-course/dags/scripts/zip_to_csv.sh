#!/bin/bash

export YEAR=${1}
export MONTH=${2}
export FILE_PATH=${3}

cd $FILE_PATH
unzip $YEAR$month.zip
mv *ONTIME.csv $YEAR$month.csv

