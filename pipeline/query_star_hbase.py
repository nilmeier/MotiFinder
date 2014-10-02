import starbase
# see http://pythonhosted.org//starbase/
import pprint
import re
import sys
import time

#entire pipeline name
pipeline_name=sys.argv[1]

# these should all be defaulted to false unless debugging----
debug=False
newtables=False    # True if a new table needs to be created.
deletetables=False # True if tables need to be deleted.
#routine cleaning operation for debugging .... production code shouldn't need this:
cleantables_before_running=False
#----------------------------------------------------------

#creating a connection to HBase:
hbase = starbase.Connection(host='0.0.0.0',port=20550) #connect to Hbase on localhost

#table names
main_table  = 'main_'+pipeline_name+'_table'
motif_table = 'motif_'+pipeline_name+'_table'



hbase_main_table  = hbase.table(main_table)
hbase_motif_table = hbase.table(motif_table)

hbase_motif_table_batch=hbase_motif_table.batch()
hbase_main_table_batch=hbase_main_table.batch()


# table creation and deletion utilities (jndb: needs to be moved!)

# creating tables (s/b a one time operation)
if newtables:
    hbase_motif_table.create('m')
    hbase_main_table.create('m')

# deleting tables (s/b a one time operation)
if deletetables:
    print "warning...about to delete stuff!"
    import rlcompleter; import readline; readline.parse_and_bind("tab: complete")
    print "\n"; import db,code; code.interact(local=locals())
    print "deleting tables..."
    hbase_motif_table.drop()
    hbase_main_table.drop()
    print "need to restart"
    exit()

# opening existing table made from hbase shell:

if cleantables_before_running:  #this will delete all rows before loading new stuff in
    print "warning...about to delete stuff!"
    import rlcompleter; import readline; readline.parse_and_bind("tab: complete")
    print "\n"; import db,code; code.interact(local=locals())
    rf = '{"type": "RowFilter", "op": "EQUAL", "comparator": {"type": "RegexStringComparator", "value": "^"}}'
    a = hbase_main_table.fetch_all_rows(with_row_id=True,filter_string=rf)
    row_keys_main=[]

    for line in a:
        row_keys_main.append(line.keys()[0])

    for rowkey in row_keys_main: 
        hbase_main_table.remove(rowkey)

    a = hbase_motif_table.fetch_all_rows(with_row_id=True,filter_string=rf)
    row_keys_motif=[]
    
    for line in a:
        row_keys_motif.append(line.keys()[0])

    for rowkey in row_keys_motif:
        hbase_motif_table.remove(rowkey)

    print "exiting"
    exit()

    print "data removed from tables!"
    exit()

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# query starts here:


#this is very special starbase regex stuff that works while scanner is in dev:
rf = '{"type": "RowFilter", "op": "EQUAL", "comparator": {"type": "RegexStringComparator", "value": "^"}}'
#a=hbase_main_table.fetch_all_rows(with_row_id=True,filter_string=rf)
#a=hbase_main_table.fetch_all_rows(with_row_id=True,filter_string=rf)


#  printing out full table when it is small enough
# row_keys_main=[]
# for line in a:
#     print line.keys()[0]
#     row_keys_main.append(line.keys()[0])
# 


# printing rows:

#print "\n\n\n - - - - printing main table - - - "
#for row in row_keys_main:
#    print"gene ID:", row , '\nsequence: ', hbase_main_table.fetch(row_keys_main[0])['m']['sequence']
#print "\n\n"
# --------------------------------------------------------

t0=time.time()
print "loading rows"
a=hbase_motif_table.fetch_all_rows(with_row_id=True,filter_string=rf)
t1=time.time()
row_keys_motif=[]
print "time to load: "+ str(t1-t0)


t0=time.time()
row_keys_motif=[]

#JN DEBUG LINE ================================================
# import rlcompleter; import readline; readline.parse_and_bind("tab: complete")
# print "jndb\n"; import db,code; code.interact(local=locals())
#JN DEBUG LINE =================================================
#for line in a:
#    print line.keys()[0]


##JN DEBUG LINE ================================================
#     import rlcompleter; import readline; readline.parse_and_bind("tab: complete")
#     print "jndb\n"; import db,code; code.interact(local=locals())
#JN DEBUG LINE =================================================
#    row_keys_motif.append(line.keys()[0])

# hard coding motifs for db:
row_keys_motif=['GATTACA','TATTACA','GATTAAC','ACATTAG']

t1=time.time()
print "time to load: "+ str(t1-t0)



# row_keys_motif=['GATTACA','TATTACA','GATTAAC','ACATTAG']

print "- - - - printing lookup table - - - "

for motif_row in row_keys_motif:

    t0=time.time()
    print "looking up columns for ", motif_row
#    motif_columns = hbase_motif_table.fetch(motif_row)['m']
    t1=time.time()
    i_column=0
    hbase_motif_string='notNone'

#note this will only work if columns are consecutive!!!!  This and the column counter need to be fixed!
    print " = = = = = = = = = = = = = = = = = = = = = = =\n"
    print "\nmotif: ", motif_row, " = = = = = = = = = = = = = =    \n"
    while hbase_motif_string !=None:
        i_column+=1
        if i_column>50: break 
        column_key='m:gene_id'+str(i_column)
        sequence_column_key='s:sequence'+str(i_column)
        check=hbase_motif_table.fetch(motif_row,column_key)
        if check==None: break

        hbase_motif_string=check['m'].values()[0]
          
          
        sequence=hbase_motif_table.fetch(motif_row,sequence_column_key)['s'].values()[0]
        gene_id=re.split('\t',hbase_motif_string)[0]

        #getting the location list, which is to the right of the tab
        print hbase_motif_string, 

        location_string=re.split('\t',hbase_motif_string)[1]
        location_stringlist=location_string.split('[')[1].split(']')[0].split(',')
        location_list=[]
        for location_string in location_stringlist:
            location=int(location_string)
            print i_column ,' ) '+ gene_id + 'at '+ str(location)

            print "  "+ sequence
            print " "+" "*(location)+"^"+motif_row+"^"
            print "" 


