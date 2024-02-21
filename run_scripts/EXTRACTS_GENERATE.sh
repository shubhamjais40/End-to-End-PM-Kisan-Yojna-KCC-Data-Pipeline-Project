#!bin/bash

read -p "Enter start date for extracts in format YYYYMMDD:" start_date


year=${start_date::4}
echo $year
