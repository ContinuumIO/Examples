from disco.job import Job
from disco.worker.classic.func import chain_reader
from disco.core import result_iterator

class WordCount(Job):
    
    partitions = 2
    input=["hemingway.txt",] #hemingway collected works
                    
    @staticmethod
    def map(line, params):
        import string
        for word in line.split():
            strippedWord = word.translate(string.maketrans("",""), string.punctuation)
            yield strippedWord.lower(), 1

    @staticmethod
    def reduce(iter, params):
        from disco.util import kvgroup
        for word, counts in kvgroup(sorted(iter)):
            yield word, sum(counts)
 
class DTCount(Job):
    map_reader = staticmethod(chain_reader)
    
    @staticmethod
    def map((word,count), params):
        import nltk
        pos_tag = nltk.pos_tag([word]) #[(word, pos)]
        if pos_tag[0][1] == 'DT':
            yield word, count
    
    @staticmethod
    def reduce(dt_iter,out,params):
        from disco.util import kvgroup
        for word, counts in kvgroup(sorted(dt_iter)):
            out.add(word,sum(counts))

if __name__ == "__main__":
    from MapReduce_CountDT_Chain import WordCount
    from MapReduce_CountDT_Chain import DTCount
    
    wordcount = WordCount().run()
    dtCount = DTCount().run(input=wordcount.wait())
    
    filePath = '/tmp/' #FILL IN
    out_numerical = open(filePath+'DT-SortNumerically.txt', 'w')
    
    dtCountLst = []
    totalCount = 0
    for (word, counts) in result_iterator(dtCount.wait(show=False)):
        totalCount += counts
        print word, counts
        dtCountLst.append((word,counts))
     
    sortedDTCount = sorted(dtCountLst, key=lambda count: count[1],reverse=True)
    
    for word, counts in sortedDTCount:
        out_numerical.write('%s \t %d \t %.2e\n' % (str(word), int(counts), counts*1.0/totalCount) )
   
    out_numerical.close()

    
    
