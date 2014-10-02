import pysam
import re
import os
import sys

# This is a simple conversion utility, giving gene id and sequence in a text file.
# This will be run in serial, so no fancy sorting or finding unique keys here.  The 
# unique keys can be done in a MapReduce job.
# For now, the row number will be appended to the gene id to make it unique.

input_filename=sys.argv[1]  #no extension
output_filename=sys.argv[2]


out=open(output_filename +'.tsv','w')

ext_list=[".bam", ".bam.bai"]
for ext in ext_list:
    if ( not(os.path.isfile(input_filename + ext) ) ):
        print input_filename + ext + " not present "
        exit()


i_ref=0
samfile =pysam.Samfile(input_filename+".bam","rb")

for alignedread in samfile.fetch('20'):
    i_ref+=1 
    out.write(alignedread.qname + '-' + str(i_ref)+'\t'+ alignedread.seq+"\n") 

out.close()
#transferring decompressed file to hdfs
hadoop_directory="/user/nilmeier/genes_tsv"

# this should be a one time directory creation, but here it is for reference:
#os.system("sudo -u hdfs hadoop fs -mkdir "+hadoop_directory)

#print "copying to "+ hadoop_directory
#os.system("sudo -u hdfs hadoop fs -put "+ filename+".tsv" + hadoop_directory)

