set -x # prints commands as they are executed

## decompress the raw data file and make a copy into data $2 is the name of the raw data
#echo decompressing $2 and copying to data/$1.tsv
#python bam_to_tsv.py rawdata/$2 data/$1 
#
## parse the raw .bam file on lfs (commands not listed here)
## assuming the input file is $1.tsv
#
## # create the directory for the input, intermediate, and output data:
#hdfs dfs -mkdir /user/nilmeier/$1 
## 
## # copy the raw tsv to hdfs
#hdfs dfs -put data/$1.tsv /user/nilmeier/$1
## 
## # run the first job and store the output in $1.mr1.out
#python mr1_load_tsv_assign_motifs.py -r hadoop --hadoop-bin /usr/bin/hadoop --no-output hdfs:///user/nilmeier/$1/$1.tsv -o hdfs:///user/nilmeier/$1/$1.mr1.out >& data/$1.mr1.log
## 
## # run the second job and store the output in $1.m2.out
#python mr2_collect_motifs.py -r hadoop --hadoop-bin /usr/bin/hadoop --no-output hdfs:///user/nilmeier/$1/$1.mr1.out/ -o hdfs:///user/nilmeier/$1/$1.mr2.out >& data/$1.mr2.log
#
## run the third job and store in $1.m3.out
#python mr3_count_motifs.py -r hadoop --hadoop-bin /usr/bin/hadoop --no-output hdfs:///user/nilmeier/$1/$1.mr2.out/ -o hdfs:///user/nilmeier/$1/$1.mr3.out >& data/$1.mr3.log
#
### load the data from hdfs to hbase (I think it needs to be retooled for larger datasets) 
#pyspark mr_to_star_hbase_via_spark.py $1

# query all motifs (for offline debugging)
#python query_star_hbase.py $1 > $1.fullmap.out
