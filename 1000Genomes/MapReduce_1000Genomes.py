from disco.core import Job, result_iterator

def read_coverage_map(rec, params):
    ref, read = rec
    yield '%s:%d' % (ref, read.pos), read.qlen



def chr_partition(key, nrp, params):
    chr = key.split(':')[0]
    if chr == 'X':      return 24
    elif chr == 'Y':    return 25
    elif chr == 'MT':   return 0
    else:               
        try:
                return int(chr)
        except ValueError:
                pass
                #print chr, key

def coverage_reduce(reduce_iter, params):
    import numpy
    chrs = { # Chromosome sizes
        '1':250000000,
        '2':250000000,
        '3':200000000,
        '4':200000000,
        '5':200000000,
        '6':200000000,
        '7':160000000,
        '8':150000000,
        '9':150000000,
        '10':150000000,
        '11':150000000,
        '12':150000000,
        '13':150000000,
        '14':150000000,
        '15':150000000,
        '16':100000000,
        '17':100000000,
        '18':100000000,
        '19':100000000,
        '20':100000000,
        '21':100000000,
        '22':100000000,
        'X':200000000,
        'MT':250000000,
        'Y':100000000 }
    
    p, l = iter(reduce_iter).next()
    chr, pos = p.split(':')
    c = numpy.zeros(chrs[chr])
    
    for p, l in reduce_iter:
        chr, pos = p.split(':')
        pos = int(pos); l = int(l)
        c[pos:pos+l] += 1

    yield (chr, ' '.join((str(int(i)) for i in c)))


def sam_url_reader(stream, size, url, params):
    import tempfile
    import pysam
    cache = tempfile.NamedTemporaryFile(dir='/mnt')
    
    BLOCK_SIZE = 4*(1024**2)
    block = stream.read(BLOCK_SIZE)
    while block != "":
        cache.write(block)
        block = stream.read(BLOCK_SIZE)

    sam = pysam.Samfile(cache.name)
    
    for read in sam:
        yield (sam.getrname(read.tid), read)
    
    sam.close()
    cache.close()
    

job = Job().run(
    input = ['http://s3.amazonaws.com/1000genomes/data/HG00096/alignment/HG00096.chrom11.ILLUMINA.bwa.GBR.low_coverage.20111114.bam',],
    #input = ['http://s3.amazonaws.com/1000genomes/data/HG00096/alignment/HG00096.mapped.ILLUMINA.bwa.GBR.low_coverage.20111114.bam'],
    map_reader = sam_url_reader,
    partition = chr_partition,
    partitions = 26,
    map=read_coverage_map,
    reduce=coverage_reduce)
    

filePath = '/mnt/'
for chr, coverage in result_iterator(job.wait(show=True)):
    out = open(filePath+chr+'_coverage-HG00096.out', 'w')
    out.write('%s %s\n' % (chr, coverage))

import os
from data_binner import makePlot

fileHandleList = (fname for fname in os.listdir('/mnt') if fname.endswith('.out'))
map(makePlot,fileHandleList)


