#!bin/bash

while getopts :t:m:y:s:e:o: flag
do
    case "${flag}" in
        t) extracts_type=${OPTARG};;
        m) month=${OPTARG};;
        y) year=${OPTARG};;
        s) start_month=${OPTARG};;
        e) end_month=${OPTARG};;
        o) outdir=${OPTARG};;
    esac
done

echo "Params for runtime are:"
echo "extracts_type: $extracts_type";
echo "month: $month";
echo "year: $year";
echo "start: $start_month";
echo "end: $end_month";
echo "out: $outdir"


if [ $extracts_type = "monthly" ] || [ $extracts_type = "MONTHLY" ] && [ $# = 8 ]; then
    echo "monthly extracts will run"
    python3 ../src/run.py $extracts_type -y $year -m $month -out $outdir

elif [ $extracts_type = "adhoc" ] || [ $extracts_type = "ADHOC" ] && [ $# = 10 ]; then
    echo "Adhoc extracts will run.."
    python3 ../src/run.py $extracts_type -y $year -sm $start_month -em $end_month -out $outdir
else
    echo "please enter valid args to process extracts.."
fi


