#!/bin/bash

function runWebApp
{
   python src/webapp/pyspy_restful_api.py
}

function unitTests
{
  nosetests
}

OPTIONS="RunWebApp UnitTesting Exit"
select opt in $OPTIONS; do
 if [ "$opt" = "RunWebApp" ]; then
  runWebApp
  exit
elif [ "$opt" = "UnitTesting" ]; then
  unitTests
  exit
else
  clear
  echo bad option
fi
done