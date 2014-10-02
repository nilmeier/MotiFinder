set -x # prints commands as they are executed

# this has a set of commands to run offline tests of the scripts.  It is primarily for map reduce testing, 
# and also to test the hbase query outside of the web interface.

# decompress the raw data file and make a copy into data $2 is the name of the raw data
# echo decompressing $2 and copying to data/$1.tsv
#python bam_to_tsv.py rawdata/$2 data/$1 

#note...the HG96 example is too big to test offline, so use the first 5000 lines, found in HG96.top5k.tsv

# # run the first job and store the output in $1.mr1.out
cat data/$1.tsv | python mr1_load_tsv_assign_motifs.py -o data/$1.mr1.out >& data/$1.mr1.offline.log
# # 

# # # run the second job and store the output in $1.m2.out
cat data/$1.mr1.out/part-00000 | python mr2_collect_motifs.py -o data/$1.mr2.out >& data/$1.mr2.offline.log
# 
# # run the third job and store in $1.m3.out
cat data/$1.mr2.out/part-00000 | python mr3_count_motifs.py -o data/$1.mr3.out >& data/$1.mr3.offline.log

 
# # pyspark loads from hdfs, so no offline version is used
# # load the data from hdfs to hbase (I think it needs to be retooled for larger datasets) 
# #pyspark mr2out_to_star_hbase_via_spark.py $1

 
# # query all motifs 
# python query_star_hbase.py $1 > $1.fullmap.out
