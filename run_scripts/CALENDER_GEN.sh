#!bin/bash

start=$(date +%s)

while getopts :s:e:f: flag
do
    case "${flag}" in
        s) start=${OPTARG};;
        e) end=${OPTARG};;
        f) freq=${OPTARG};;
    esac
done

echo "$start , $end , $freq"

python3 ../src/calender_generator.py $start $end $freq

end=$(date +%s)

echo "Elapsed Time: $(($end-$start)) seconds"


#Command sample: sh CALENDER_GEN.sh -s 2024/01/01 -e 2024/02/29 -f D
