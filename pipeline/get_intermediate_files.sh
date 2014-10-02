set -x 

#gets files from hdfs, renames them and writes to data directory

## create the directory for the input, intermediate, and output data:
rm $1.mr1.out
hdfs dfs -get /user/nilmeier/$1/$1.mr1.out/part-00000  data/$1.mr1.out
## copy the raw tsv to hdfs
rm $1.mr2.out
hdfs dfs -get /user/nilmeier/$1/$1.mr2.out/part-00000 data/$1.mr2.out

rm $1.mr3.out
hdfs dfs -get /user/nilmeier/$1/$1.mr3.out/part-00000 data/$1.mr3.out




