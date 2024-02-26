#!bin/bash

start=$(date +%s)
filename='/home/hadoop/pmkcc_project/PM-Kisan-Yojna-KCC-Project/ARCHIVE/blocks_coordinates.csv'

if [ -f $filename ]; then
    echo 'The file already exists.Hence aborting New Generation attempt'
else
    echo 'The file does not exist.'
    echo "Generating Blocks Coordinates from Files in Archive"
    python3 ../src/blockCoordinates_generator.py  

fi




end=$(date +%s)

echo "Elapsed Time: $(($end-$start)) seconds"
