#!bin/bash

start=$(date +%s)

cd ../src/

echo `pwd`

spark-submit kccMerged.py

end=$(date +%s)

echo "Elapsed Time: $(($end-$start)) seconds"