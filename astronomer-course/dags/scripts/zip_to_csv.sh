#!/bin/bash

export YEAR=${1}
export MONTH=${2}
export FILE_PATH=${3}

cd $FILE_PATH
unzip /tmp/$YEAR$MONTH.zip
mv *ONTIME.csv /tmp/$YEAR$MONTH.csv

