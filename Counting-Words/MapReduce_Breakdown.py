import string


def mapFunc(corpus):
    
    keyValue = [] 
    for word in corpus.split():
        #clean up word (lower text and remove punctuation
        wordNoPunct = word.translate(string.maketrans("",""), string.punctuation)
        lower_word = wordNoPunct.lower()
        
        
        print '||', lower_word, '||', hash(lower_word), '||', hash(lower_word) % 5, '||'
        
        keyValue.append((lower_word,1))
    return keyValue
        

mr_turkentine = "Of course you don't know. You don't know because only I know.\
                 If you knew and I didn't know, then you'd be teaching me instead\
                 of me teaching you - and for a student to be teaching his teacher \
                 is presumptuous and rude. Do I make myself clear?"

keyVal = mapFunc(mr_turkentine)
