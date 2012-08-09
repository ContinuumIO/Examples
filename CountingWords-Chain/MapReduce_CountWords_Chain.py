from disco.job import Job
from disco.worker.classic.func import chain_reader
from disco.core import result_iterator

class WordCount(Job):
    
    partitions = 2
    #input=["faulkner.txt",] #faulkner collected works
    input=["hemingway.txt",] #hemingway collected works
                    
    @staticmethod
    def map(line, params):
        import string
        for word in line.split():
            strippedWord = word.translate(string.maketrans("",""), string.punctuation)
            yield strippedWord, 1

    @staticmethod
    def reduce(iter, params):
        from disco.util import kvgroup
        for word, counts in kvgroup(sorted(iter)):
            yield word, sum(counts)
 
class PosCount(Job):
    map_reader = staticmethod(chain_reader)
    
    @staticmethod
    def map((word,count), params):
        import nltk
        pos_tag = nltk.pos_tag([word]) #[(word, pos)]
        yield pos_tag[0][1],count

    
    @staticmethod
    def reduce(pos_iter,out,params):
        from disco.util import kvgroup
        for pos, counts in kvgroup(sorted(pos_iter)):
            out.add(pos,sum(counts))

if __name__ == "__main__":
    from MapReduce_CountWords_Chain import WordCount
    from MapReduce_CountWords_Chain import PosCount
    from posTagDict import posTagDict
    
    wordcount = WordCount().run()
    posCount = PosCount().run(input=wordcount.wait())
    
    filePath = '/tmp/' #FILL IN
    out_numerical = open(filePath+'POS-SortNumerically.txt', 'w')
    
    posCountLst = []
    totalCount = 0
    for (pos, counts) in result_iterator(posCount.wait(show=False)):
        print pos, posTagDict[pos], counts 
        totalCount += counts
        posCountLst.append((pos,counts))
     
    sortedPOSCount = sorted(posCountLst, key=lambda count: count[1],reverse=True)
    
    for pos, counts in sortedPOSCount:
        out_numerical.write('%s \t %s \t %d \t %.2e\n' % (str(pos), str(posTagDict[pos]), int(counts), counts*1.0/totalCount) )
   
    out_numerical.close()

    
