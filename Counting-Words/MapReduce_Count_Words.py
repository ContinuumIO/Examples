#http://www.gutenberg.org/cache/epub/244/pg244.txt (Study in Scarlet)
#http://www.gutenberg.org/cache/epub/2097/pg2097.txt (Sign Of Four)
#http://www.gutenberg.org/cache/epub/1661/pg1661.txt (The Adventures of Sherlock Holmes Collected Works

#http://www.gutenberg.org/cache/epub/863/pg863.txt (Agtha Christie -- Poirot)

from disco.core import Job, result_iterator

def map(line, params):
    import string
    for word in line.split():
        strippedWord = word.translate(string.maketrans("",""), string.punctuation)
        yield strippedWord, 1
        
def reduce(iter, params):
    from disco.util import kvgroup
    for word, counts in kvgroup(sorted(iter)):
        yield word, sum(counts)
        
if __name__ == '__main__':
    job = Job().run(
                    #input=["http://www.gutenberg.org/cache/epub/2097/pg2097.txt"],
                    #input=["http://www.gutenberg.org/cache/epub/2097/pg2097.txt", "http://www.gutenberg.org/cache/epub/244/pg244.txt",],
                    #input=["http://www.gutenberg.org/cache/epub/1661/pg1661.txt"],
                    
                    input=["http://www.gutenberg.org/cache/epub/863/pg863.txt",],
                    
                    map=map,
                    reduce=reduce)
                    
    
    filePath = '/tmp/' #FILL IN
    out_numerical = open(filePath+'Words-SortNumerically.txt', 'w')
    out_abc = open(filePath+'Words-SortAlphabetically.txt', 'w')
    
    wordCount = []
    for word, count in result_iterator(job.wait(show=True)):
        out_abc.write('%s \t %d\n' % (str(word), int(count)) )
        wordCount.append((word,count))
     
    #sorted list from an iterable.  lambda function returns the count -- position 1 of the tuple we created.
    sortedWordCount = sorted(wordCount, key=lambda count: count[1],reverse=True)
    
    for word, count in sortedWordCount:
        out_numerical.write('%s \t %d\n' % (str(word), int(count)) )
   
    out_abc.close()
    out_numerical.close()

    question_words = ['who', 'where','what', 'when', 'why', 'how']
    modal_verbs = ['can', 'could', 'need', 'may', 'might', 'must', 'shall', 'will']
    
    print [word for word in sortedWordCount if word[0] in question_words]
    print [word for word in sortedWordCount if word[0] in modal_verbs]
