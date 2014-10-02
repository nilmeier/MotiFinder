from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol  # JSONProtocol, JSONValueProtocol
import re


debug=False

class MRUser(MRJob):
    
    def mapper_extract_motifs(self, _, line):
        if debug: print line
        motif_key = line.split("-")[0].split('"')[1]
        # a comma indicates more than one motif per row
        count=1+line.count(',')
        yield motif_key, count

    def reducer_count_motif_keys(self, key, values):
       yield key, sum(values)


    def steps(self):
        return [

            self.mr( mapper=self.mapper_extract_motifs, 
                    reducer=self.reducer_count_motif_keys)

        ]

if __name__ == '__main__':
    MRUser.run()
