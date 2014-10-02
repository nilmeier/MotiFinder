import random

from collections import OrderedDict
from operator import itemgetter

import numpy as np
import matplotlib.pyplot as plt
import mpld3


def sample_plot():
    # hard coding row_keys for now:
    row_keys_motif=['GATTACA','TATTACA','GATTAAC','ACATTAG']
    # creating a 'count' that will 
    num2base=['A','G','T','C']
    count={}

    count['GATTACA']=300
    count['TATTACA']=400
    count['GATTAAC']=500
    count['ACATTAG']=200

    #  adding a bunch more for a histogram:
    for i in range(100):    # number of random motifs
        motif=''
        num_tot=1
        for j in range(7):  #length of motif
           #generating some weird formula for the count:
           num=random.randrange(4)
           num_tot=( (num+1)*num_tot )
           motif=motif+num2base[random.randrange(4)]
           
        row_keys_motif.append(motif)
        count[motif]=num_tot




   # for motif in row_keys_motif:
   #     print motif+ ": "+str( count[motif] )

    #for (key,value) in OrderedDict(sorted(count.items(), key=itemgetter(1),reverse=True)):
    #     print key, value

    hist=sorted(count.values(),reverse=True)

    test=sorted(count.items(), key=lambda x:x[1])

    fig, ax = plt.subplots(1)
    ax.plot(hist)

    # plt.savefig('hist.png',bbox_inches='tight')
    mpld3_data = mpld3.fig_to_dict(fig)
    return mpld3_data
    #JN DEBUG LINE ================================================
    #import ipdb;ipdb.set_trace()           
    #JN DEBUG LINE =================================================


