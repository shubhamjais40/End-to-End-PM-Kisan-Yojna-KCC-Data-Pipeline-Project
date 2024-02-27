#!bin/bash

start=$(date +%s)

spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.0 ../src/kccMerged.py

end=$(date +%s)

echo "Elapsed Time: $(($end-$start)) seconds"