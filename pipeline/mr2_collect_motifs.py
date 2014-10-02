from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol  # JSONProtocol, JSONValueProtocol
import json
import re

# this can be generated in situ and run on mapper

debug=False

class MRUser(MRJob):
    
    def mapper_enumerate_motif_locations(self, _, line):
        # this mapper will decouble the double motif-gene_id index 
        # to be collected, and create a JSON like column list.
        if debug: print line
        motif_key = line.split("-")[0].split('"')[1]
        # contains the full list of motifs to be parsed
        gene_id_and_edge_list = re.subn('"'+motif_key+"-","",line.strip())[0]
        gene_id_and_edge_list = re.subn('"','',gene_id_and_edge_list)[0]
        if debug:  print gene_id_and_edge_list 
           
        if debug: print motif_key , "  " , gene_id_and_edge_list
        yield motif_key, gene_id_and_edge_list

    def reducer_collect_motif_keys(self, key, values):
        row_counter=0
        column_counter=0
        for value in values:
             # getting the gene_id from the string
             line_prefix=re.subn('"','',value.split('\t')[0])[0]
             # getting the list string
             list_string=re.subn('"','',value.split('\t')[1])[0]
             # stripping square brackets and splitting by commas to get the list
             tmp_line=re.subn('\]','' , re.subn('\[','',list_string)[0] )[0]
             same_row_location_list=tmp_line.split(", ")
             # creating a comma separated list in square brackets 
             location_list_string='['
             row_counter=0
             for location in same_row_location_list:
                 row_counter+=1
                 column_counter+=1
                 if row_counter>1:
                     location_list_string+=','
                 location_list_string +=location+ '|c'+str(column_counter)
             location_list_string+=']'
             # placing the list after an @ delimiter
             outvalue=line_prefix+'@'+ location_list_string

             #need to split the cases where multiple columns appear on the 
             yield key, outvalue 

    def steps(self):
        return [

            self.mr( mapper=self.mapper_enumerate_motif_locations, 
                    reducer=self.reducer_collect_motif_keys)

        ]


if __name__ == '__main__':
    MRUser.run()
