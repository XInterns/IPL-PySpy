#!/bin/bash

function runWebServer
{
   python src/webapp/pyspyWebServer.py
}

function unitTests
{
  nosetests
}

OPTIONS="RunWebServer UnitTesting Exit"
select opt in $OPTIONS; do
 if [ "$opt" = "RunWebServer" ]; then
  runWebServer
  exit
elif [ "$opt" = "UnitTesting" ]; then
  unitTests
  exit
else
  clear
  echo bad option
fi
done
