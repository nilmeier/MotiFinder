from pyspark import SparkContext, SparkConf
import starbase
import re
import sys
import time

update_main=False
update_motif=False
update_motif_count=True
bufferlength=5000 #this is the starbase recommended buffer length 
create_new_tables=True # will create a new set of tables

#entire pipeline name
pipeline_name=sys.argv[1]

#table names
main_table  = 'main_'+pipeline_name+'_table'
motif_table = 'motif_'+pipeline_name+'_table'

#creating a connection to HBase:
hbase = starbase.Connection(host='0.0.0.0',port=20550) #connect to Hbase on localhost

#creating table objects
hbase_main_table  = hbase.table(main_table)
hbase_motif_table = hbase.table(motif_table)

#creating tables in hbase if necessary:
if create_new_tables:
    hbase_main_table.create('m')
    hbase_motif_table.create('m')

#creating batch table object
hbase_motif_table_batch=hbase_motif_table.batch()
hbase_main_table_batch=hbase_main_table.batch()

# creating spark context
sc = SparkContext("local", "pyspark")

# loading output from 2nd mapreduce job (mr2) as input file to build motif table
# hdfs_motif_filename="/user/nilmeier/"+pipeline_name+"/"+pipeline_name+".mr2.out/part-00000"

# loading output from 1st mapreduce job (mr2) as input file to build motif table
hdfs_motif_filename="/user/nilmeier/"+pipeline_name+"/"+pipeline_name+".mr2.out/part-00000"
hdfs_motif_count_filename="/user/nilmeier/"+pipeline_name+"/"+pipeline_name+".mr3.out/part-00000"

print "loading rdd for "+ hdfs_motif_filename+"\n"
motif_file = sc.textFile(hdfs_motif_filename)
motif_count_file=sc.textFile(hdfs_motif_count_filename)

# loading input tsv file
hdfs_main_filename="/user/nilmeier/"+pipeline_name+"/"+pipeline_name+".tsv"
print "loading rdd for "+ hdfs_main_filename+"\n"
main_file = sc.textFile(hdfs_main_filename)

def write_to_motif_table_batch(line):
    # warning...this assumes:
    # the file is read serially (no mapper here)
    # 
    global i_line    
    global bufferlength
    global lastline
    # stripping double quotes

    # motif is to the left of the first tab and in quotes:
    motif = re.subn('"','',line.split('\t')[0])[0]    
    
    #the sequence id needs to be stripped of quotes and carriage returns:
    mr_line=re.subn('"','',line.strip())[0].split('\t')[1]
    #an @ delimiter is between gene id and a list of locations
    gene_id=mr_line.split('@')[0]

    # getting sequence from main table for normalization:
    sequence = hbase_main_table.fetch(gene_id,'m:sequence')['m']['sequence']
                                                                                
    # getting the encoded list of locations for that row
    list_string=mr_line.split('@')[1]
    # stripping square brackets and splitting by commas to get the list
    tmp_line=re.subn('\]','' , re.subn('\[','',list_string)[0] )[0]
    # splitting on commas to get a list   
    same_row_location_list=tmp_line.split(",")
    # print "same row ", same_row_location_list


    row_dictionary={}
    output_location_list=same_row_location_list[0]
    row_counter=0
    
    #note...the columns are counted individually from the mr job, so we will 
    # maintaing that convention here.

    for annotated_location in same_row_location_list:
        row_counter+=1
        if row_counter>1:
            location_list_string+=','
        # location is to the left of the pipe in the list
        location=annotated_location.split('|')[0]
        # column number is to the right of the pipe
        column_number=int(re.subn('c','',annotated_location.split('|')[1])[0])
#        print 'column:', column_number, 'location ', location
        # rebuilding column_list in original format
        #output_location_list=re.subn('|c'+str(column_number),'',output_location_list)[0]
        location_list_string='['+str(location)+']'
        hbase_value_string=gene_id+'\t'+location_list_string
#        print motif+'\t', hbase_value_string 

        row_dictionary={}
        row_dictionary.update({'m:gene_id'+str(column_number):hbase_value_string,'s:sequence'+str(column_number):sequence})
        #  print sequence
        hbase_motif_table_batch.insert( motif,row_dictionary ) 
        #                                                                                                                 
        i_line=i_line+1                                                                                            
        #
        if (i_line%bufferlength==0 or i_line==lastline): 
            print "writing  line ", str(i_line)+" of "+ str(lastline)+ "lines"
            hbase_motif_table_batch.commit(finalize=True)
    #        print "done commiting in ", commit_tf-commit_t0,"s \n\n"    

             

def write_to_main_table_batch(line):
    global i_line
    global bufferlength 
    global lastline

    gene_id=line.split('\t')[0]
    #building main_list from io (starbase doesn't have scan working yet)
    sequence=line.strip().split('\t')[1]
    hbase_main_table_batch.insert(gene_id,{'m:sequence':sequence})
    i_line+=1
    if (i_line%bufferlength==0 or i_line==lastline):
        print "writing  line ", str(i_line), "of " , str(lastline), " lines"     
        hbase_main_table_batch.commit(finalize=True)
# 

def write_motif_counts_batch(line):
    global i_line
    global bufferlength 
    global lastline

    motif=re.subn('"','',line.split('\t')[0])[0]
    count=line.split('\t')[1]
    print "motif", motif , " count ", str(count)
    #building main_list from io (starbase doesn't have scan working yet)
    hbase_main_table_batch.insert(motif,{'m:total':count})
    i_line+=1
    if (i_line%bufferlength==0 or i_line==lastline):
        print "writing  line ", str(i_line), "of " , str(lastline), " lines"     
        hbase_main_table_batch.commit(finalize=True)


#========== main updates  ========================
if update_main:
    print "updating main table"
    i_line=0
    writecount=0
    lastline=main_file.count()
    commit_t0=time.time()
    print "there are "+ str(lastline)+"lines"
    main_file.foreach(write_to_main_table_batch) 

#=========== motif updates ========================
if update_motif:
    print "updating motif table"
    i_line=0
    commit_t0=time.time()
    lastline=motif_file.count()
    motif_file.foreach(write_to_motif_table_batch) 
    print "done with motif batches...", str(time.time()-commit_t0), "s"  

# appending total count to motif table ===========
if update_motif_count:
    print "updating motif table with counts...not implemented yet"
    i_line=0
    commit_t0=time.time()
    lastline=motif_count_file.count()
    motif_count_file.foreach(write_motif_counts_batch) 
# 

