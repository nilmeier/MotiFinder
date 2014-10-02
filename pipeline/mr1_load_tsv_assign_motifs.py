from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol  # JSONProtocol, JSONValueProtocol


i_ref=0; maxref=1e99

motiflist=['GATTACA','TATTACA','GATTAAC','ACATTAG']

debug=False

class MRUser(MRJob):

    def mapper(self, _, line):
        if debug: print line
        gene_id=line.split('\t')[0]
        sequence=line.split('\t')[1].strip()

        for motif in motiflist:
           window_length = len(motif) #size of sliding window to compare
           seq_length = len(sequence)
           if debug:
               print "looking for motif: ", motif + " in: " 
               print sequence
           for i_window in range(seq_length-window_length+1):
               # a sliding window is used here...not necessarily for efficiency, but 
               # to allow for the possiblity of generating similarity searches
               test_sequence=sequence[i_window:i_window+window_length]
               
               if debug:  print "motif, seq:", motif,',', test_sequence
               if motif==test_sequence:
                   if debug:
                       print motif, sequence[i_window:i_window+window_length]
                       print " "+sequence

                   yield motif+'-'+gene_id, i_window
        
    def reducer(self, key, values):
       yield key, list(values) 

if __name__ == '__main__':
    MRUser.run()
