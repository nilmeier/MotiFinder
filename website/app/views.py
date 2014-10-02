from flask import render_template, request, url_for, redirect

from app import app
import re
import starbase
import time
import json
from histogram_mockup import sample_plot

debug=False
#db = mdb.connect(user="root", host="localhost", db="world_innodb", charset='utf8')


#entire pipeline name (hard coded for now)
pipeline_name='HG96'

#table names
main_table  = 'main_'+pipeline_name+'_table'
motif_table = 'motif_'+pipeline_name+'_table'

#creating a connection to HBase:
hbase = starbase.Connection(host='0.0.0.0',port=20550) #connect to Hbase on localhost
hbase_main_table  = hbase.table(main_table)
hbase_motif_table = hbase.table(motif_table)


@app.route('/')
@app.route('/index')
def index():
    return redirect(url_for('sequence_page_test'))



@app.route('/slides/')
def slides():
    return render_template('slides.html')

   
@app.route("/db_fancy/") 
def sequence_page_test():
    motif_row = request.args.get('motif', '')
    i_start = int(request.args.get('start-row', 1))
    list_length=25
    i_end = i_start + list_length


    t0=time.time()

    sequence_list={}
    sequence_list[motif_row] = [] 
    hbase = starbase.Connection(host='0.0.0.0',port=20550) #connect to Hbase on localhost
    hbase_motif_table = hbase.table(motif_table)
    
    i_column=i_start
    if (motif_row!=''):
        hbase_motif_string='notNone'
        i_column=i_start-1
        #print motif_row
        while hbase_motif_string !=None:
            i_column+=1
            if i_column>=i_end: break # need to fix this for specified range
            column_key='m:gene_id'+str(i_column)
            sequence_column_key='s:sequence'+str(i_column)
#            print 'motif: ' + motif_row+ 'hb row:' + hbase_motif_table.fetch(motif_row)
            check=hbase_motif_table.fetch(motif_row,column_key)
            if check==None: 
                #print check; 
                break
            hbase_motif_string=check['m'].values()[0]

            full_id=hbase_motif_string #jndb fix this double assignment!  
              
            full_sequence=hbase_motif_table.fetch(motif_row,sequence_column_key)['s'].values()[0]
            gene_id=re.split('\t',hbase_motif_string)[0]
            gene_id_pre=re.split('-',gene_id)[0]
            gene_id_post=re.split('-',gene_id)[1]
            #getting the location list, which is to the right of the tab
            location_string=re.split('\t',hbase_motif_string)[1]
            location_stringlist=location_string.split('[')[1].split(']')[0].split(',')
            location_list=[]
#        num_edges=len(location_list)
            for location_string in location_stringlist:
                location=int(location_string)
 
                debug=False
                if debug: 
                    print i_column ,' ) '+ gene_id + 'at '+ str(location)
                    print "  "+ full_sequence
                    print " "+" "*(location)+"^"+motif_row+"^"
                    print "" 


                (leading_sequence,motif_sequence,trailing_sequence)=\
                 split_sequence_around_motifs(full_sequence, location+1, motif_row)

                sequence_list[motif_row].append(dict(row_count=i_column,gene_ID=gene_id_pre,index_ID=gene_id_post, sequence= full_sequence, location=location,\
                    seq1=leading_sequence,seq2=motif_sequence,seq3=trailing_sequence))


                        
    sequence_records=sequence_list[motif_row]

    t1=time.time()
    dt=round(t1-t0,3)  
    prev_set = i_start-list_length
    if prev_set < 0: prev_set = 1
    next_set = i_column 
    mpld3_data = sample_plot()
    return render_template('sequences.html', prev_set=prev_set,\
                   next_set=next_set, records=sequence_records,\
                  list_length=list_length,motif=motif_row,dt=dt, mpld3_data=json.dumps(mpld3_data))   


def split_sequence_around_motifs(sequence, location, motif):
   # 
    s1=sequence[0:location-1]
    s2=sequence[location-1: location + len(motif)-1]
    s3=sequence[location+len(motif)-1:]
    if s2!=motif: print "error"
    #print "reminder: verify index "
    return s1,s2,s3



    
