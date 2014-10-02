MotiFinder has two directories...pipeline has all of 
the backend scripts that run map reduce jobs and load
hbase with the required motif table. 

The shell script mr_pipeline.sh pipeline_name script that launches the series of jobs up to and including the loading of hbase.  Each script generates an intermediate checkpoint and log file.  There are 3 map reduce jobs, and a pyspark script that loads the hdfs output into hbase.  The mapreduce jobs are described below.  See also the diagram pipeline.pdf

uses pysam library --------------------------------------------

0) bam_to_tsv.py
   decompresses the file format and parses out gene ID and sequence.
   currently runs in serial.

uses mrjobs library ------------------------------------------
1) mr1_load_tsv_assign_motifs.py pipeline_name
   creates a list of motifs and their locations in a particular gene ID

2) mr2_collect_motif.py
   collects all (genome-wid) locations of a motif,
   assigns unique column indices, and prints one entry per row.

3) mr3_count_motifs.py 
   counts all motifs (including multiple instances on a gene ID).

uses pyspark and starbase -------------------------------------- 
4) mr_to_star_hbase_via_spark.py
   loads the three tables from hdfs and creates a motif table with 
   renormalized sequence data included, and deposits 
   this data into hbase. 

Only a very small version of the decompressed data is included for reference. 
It is small enough to be run on your local file system to see what the map reduce outputs look like.  To do this, use ./mr_pipeline_offline.sh HG96dev.top5k

Other utility scripts include:
get_intermediate_files.sh 
  downloads hdfs outputs to data directory with new names

clean_pipeline.sh
   removes all pipeline data from hdfs 


├── pipeline
│   ├── bam_to_tsv.py
│   ├── clean_mr_pipeline.sh
│   ├── data
│   │   └── HG96dev.top5k.tsv
│   ├── get_intermediate_files.sh
│   ├── mr1_load_tsv_assign_motifs.py
│   ├── mr2_collect_motifs.py
│   ├── mr3_count_motifs.py
│   ├── mr_pipeline_offline.sh
│   ├── mr_pipeline.sh
│   ├── mr_to_star_hbase_via_spark.py
│   ├── query_star_hbase.py
│   └── rawdata

a very small example tsv file is included, along with 


The website directory contains a simple web interface 
that allows the user to enter a motif and get results
in batches of 25 for the query, along with a histogram of 
the motif catalog.  The histogram still needs to be connected
to a real dataset.

