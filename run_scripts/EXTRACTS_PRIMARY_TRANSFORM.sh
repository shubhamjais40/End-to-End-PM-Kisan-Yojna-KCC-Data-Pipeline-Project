#!bin/bash

start=$(date +%s)

cd ../src/

echo `pwd` || echo `$SPARKHOME`

spark-submit --packages com.crealytics:spark-excel_2.12:3.5.0_0.20.3 extracts_clean_transform.py

end=$(date +%s)

echo "Elapsed Time: $(($end-$start)) seconds"