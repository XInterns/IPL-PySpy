#!/bin/bash

function runWebApp
{
   python src/webapp/pyspyWebServer.py
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