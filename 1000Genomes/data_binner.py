#bins data in chunks of 10000 (averages data) 

import numpy as np
import datetime


import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt

import sys,time
import math

size = 1024
BP_SIZE = int(1E3)
lst = []
window = []

checkDelims = [' ', '\t', '\n', '']


lstArray = np.zeros(1)

def makePlot(filehandle):
    try:
        f = open(filehandle,'r')
        f.read(2)
        chunk = f.read(size)
        print 'processing fle:\n'+str(filehandle)+'\n'
        while chunk !="":
            #print '.',
            if chunk[-1] in checkDelims:
                pass
            else:
                tempChunk = ''
                while True:
                    tempByte = f.read(1)
                    if tempByte in checkDelims:
                        break;
                    tempChunk += tempByte
                chunk += tempChunk

            lst.append(np.fromiter((int(x) for x in chunk.split()),'int32') )
            chunk = f.read(size)

        print 'concantenate list'

        lstArray = np.concatenate(lst)

        print 'binning data: '

        #decimate chunks of BP_SIZE
        for start in range(0,lstArray.size,BP_SIZE):
            #print '.',
            window.append(math.log(np.max(lstArray[start:start+BP_SIZE])+1)) #add 1 for log problem)

        print 'Finished binning.  Closing file'

        print 'Plotting Image'
        
        plt.figure(1)

        title = filehandle.split('.')[0]
        plt.title('Chromosome: '+title.split('_')[0],fontsize = 10)

        plt.xlabel('Maximum of Moving Window')
        plt.ylabel('Log of Coverage')
        
        plt.xlim(0,250000)
        plt.ylim(0,14)

        plt.plot(window)
        now = datetime.datetime.now()


        print 'Saving Image'
        plt.savefig(title+now.strftime("-%Y-%m-%d_%H:%M")+'.png')

        plt.figure(2)
        n, bins, patches = plt.hist(window, bins=30) 
        plt.savefig('histogram'+now.strftime("-%Y-%m-%d_%H:%M")+'.png')

        f.close()
        return 1
        
    except Exception, err:
        print err
        sys.stderr.write('ERROR: %s\n' % str(err))

        return 0

if __name__ == '__main__':
    import sys, math
    if len(sys.argv) != 2:
        sys.exit("Please provide digested BAM file")

    makePlot(sys.argv[1])
