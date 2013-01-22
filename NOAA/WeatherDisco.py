from disco.job import Job
from disco.worker.classic.func import chain_reader
from disco.worker.classic.func import nop_map
from disco.core import result_iterator
import ftplib,os, sys
 
 
class Filter1973(Job):

    partitions = 4
    input = ['raw://0']
 
    @staticmethod
    def map(stat1973,params):
        import ftplib
        
        #connect to NOAA and download list of files from 1973
        ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
        ftp.login()
        files = ftp.nlst('pub/data/gsod/1973')
 
        #yield list of files to mappers (files will be split with default partitioning)
        for f in files:
            yield (f,1)
 
        ftp.close()
 
    @staticmethod
    def reduce(stationIDs, out, params):
 from disco.job import Job
from disco.worker.classic.func import chain_reader
from disco.worker.classic.func import nop_map
from disco.core import result_iterator
import ftplib,os, sys
 
 
class Filter1973(Job):

    partitions = 4
    input = ['raw://0']
 
    @staticmethod
    def map(stat1973,params):
        import ftplib
        
        #connect to NOAA and download list of files from 1973
        ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
        ftp.login()
        files = ftp.nlst('pub/data/gsod/1973')
 
        #yield list of files to mappers (files will be split with default partitioning)
        for f in files:
            yield (f,1)
 
        ftp.close()
 
    @staticmethod
    def reduce(stationIDs, out, params):
        import ftplib,os
 
        ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
        ftp.login()
 
        path = '/tmp/weather_files_1973/'
        if not os.path.exists(path):
            os.makedirs(path)
 
        stations = []
 
        #download file station report
        for stat1973, v in stationIDs:
            cache = open(path+stat1973.split('/')[-1],'wb')
 
            try:
                ftp.retrbinary("RETR " + stat1973, cache.write, 8*1024)
            except:
                ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
                # 'Succesfully Connected...'
                ftp.login()
                ftp.retrbinary("RETR " + stat1973, cache.write, 8*1024)
             
            cache.close()
 
            #skip tar file
            if stat1973.endswith('.op.gz'):
                adapter = iopro.text_adapter(cache.name,compression='gzip',parser='csv', field_names=True)
 
                avg_temp = adapter[:]['TEMP']
                if (len(avg_temp) < 360) or (len(avg_temp) > 366):
                    continue      
                if 9999.9 in avg_temp:
                    continue
                else:
                    stations.append(cache.name.split('/')[-1][:12])
                    #store station which has good coverage for the year
 
        out.add(1,stations)
        #yield list of stations
 
        ftp.close()
 
class ChainStations(Job):
    params = 4
    map_reader = staticmethod(chain_reader)
 
    map = staticmethod(nop_map)
 
    @staticmethod
    def reduce(stationIDs, out, params):
        stations = []
        for v,statID in stationIDs:
             stations = stations+statID
 
        stations = set(stations)
        ranges = []
        step = (2013-1974)/int(params)
        for i in range(int(params)):
            if i == int(params)-1:
                ranges.append(range(1974+i*step,2013))
            else:
                ranges.append(range(1974+i*step,1974+(i+1)*step))

        
        for i,r in enumerate(ranges):
            out.add(i,(stations,r))
        
class PersistingYears(Job):
    partitions = 4
    map_reader = staticmethod(chain_reader)
    map = staticmethod(nop_map)
 
    @staticmethod
    def reduce(iter,out, params):
        import ftplib,os
 
        ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
        ftp.login()
 
        for key, StatRange in iter:
            stations = set(StatRange[0])
            ranges = StatRange[1]
            for r in ranges:
                try:
                    files = ftp.nlst('pub/data/gsod/'+str(r))
                except:
                    ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
                    # 'Succesfully Connected...'
                    ftp.login()
                    files = ftp.nlst('pub/data/gsod/'+str(r))
                 
                files = [f.split('/')[-1][:12] for f in files]
 
                stations = set(stations) & set(files)

        out.add(key,stations)
 
class StationSets(Job):
    params = 4
    map_reader = staticmethod(chain_reader)
 
    map = staticmethod(nop_map)
 
    @staticmethod
    def reduce(stationIDs, out, params):
        stationSet = set()
        for v,statID in stationIDs:
            if not stationSet:
                stationSet = stationSet.union(statID)
            stationSet = stationSet & statID
            # for stid in statID:
            #     if stid not in stationSet:
            #         stationSet.add(stid) 
        stationList = list(stationSet)
        l = len(stationList)
        
        fileout = open('/tmp/set_of_stations.csv','w')
    
        for stat in stationList:
            print >> fileout, stat

        ranges = []
        step = (2013-1974)/int(params)
        for i in range(int(params)):
            if i == int(params)-1:
                ranges.append(range(1974+i*step,2013))
            else:
                ranges.append(range(1974+i*step,1974+(i+1)*step))

        
        for i,r in enumerate(ranges):
            out.add(i,(stationList,r))

        # if l%4 == 0:
        #     for i in range(4):
        #         out.add(i,stationList[0+i*l/4:l/4*(i+1)])
        # else:
        #     for i in range(4):
        #         if i == 3:
        #             out.add(i,stationList[0+i*l/4:l])
        #         else:
        #             out.add(i,stationList[0+i*l/4:l/4*(i+1)])

class CoverageSets(Job):
    partitions = 4
    map_reader = staticmethod(chain_reader)
    map = staticmethod(nop_map)
 
    @staticmethod
    def reduce(iter,out, params):
        import ftplib,os
 
        ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
        ftp.login()

        gsod_path = 'pub/data/gsod/'
        dirpath = '/tmp/weather_files_coverage/'

        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        
        for key, StatRange in iter:
            stations = list(StatRange[0])
            ranges = StatRange[1]
            for date in ranges:
                for stat in stations:
                    print date, len(stations)
                    cache = open(dirpath+stat+'-'+str(date)+'.op.gz','wb')
                    f = gsod_path+str(date)+'/'+stat+'-'+str(date)+'.op.gz'
                   
                    try:
                        ftp.retrbinary("RETR " + f, cache.write, 8*1024)
                    except:
                        ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
                        # 'Succesfully Connected...'
                        ftp.login()
                        ftp.retrbinary("RETR " + f, cache.write, 8*1024)

                    try:
                        cache.close()
                        adapter = iopro.text_adapter(cache.name,compression='gzip',parser='csv', field_names=True)
                        avg_temp = adapter[:]['TEMP']
                    except:
                        stations.remove(stat)
                        continue                    
                    if (len(avg_temp) < 360):
                        stations.remove(stat)
                        continue       
                    if 9999.9 in avg_temp:
                        stations.remove(stat)
                        continue
                
        out.add(1,set(stations))
            
class StationSetsCoverage(Job):
    params = 4
    map_reader = staticmethod(chain_reader)
 
    map = staticmethod(nop_map)
 
    @staticmethod
    def reduce(stationIDs, out, params):
        stationSet = set()
        for v,statID in stationIDs:
            if not stationSet:
                stationSet = stationSet.union(statID)
            stationSet = stationSet & statID
            # for stid in statID:
            #     if stid not in stationSet:
            #         stationSet.add(stid) 
        stationList = list(stationSet)
        l = len(stationList)
        
        fileout = open('/tmp/set_of_stations-fullcoverage.csv','w')
    
        for stat in stationList:
            print >> fileout, stat

        
        if l%4 == 0:
            for i in range(4):
                out.add(i,stationList[0+i*l/4:l/4*(i+1)])
        else:
            for i in range(4):
                if i == 3:
                    out.add(i,stationList[0+i*l/4:l])
                else:
                    out.add(i,stationList[0+i*l/4:l/4*(i+1)])


class CalculateAverages(Job):
    partitions = 4
    sort = True
    map_reader = staticmethod(chain_reader)
    
    @staticmethod
    def map(StationSet, params):
        path = 'pub/data/gsod/'
        
        k,stats = StationSet
        for i, stid in enumerate(stats):
            for date in range(1973,2013):
                WeatherDateStat = path+str(date)+'/'+str(stid)+'-'+str(date)+'.op.gz'
                yield (str(date), WeatherDateStat)
 
    @staticmethod
    def reduce(iter, out, params):
        import numpy as np
        import ftplib,os
        import iopro,shutil
        from disco.util import kvgroup
        
        for date, WeatherDateStat in kvgroup(iter):
            
            # print 'Connecting to NOAA...'
            ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
            # print 'Succesfully Connected...'
            ftp.login()
 
            avg_temp = []
            
            stdev = 0
            SUM = 0
            mean = 0

            path = '/tmp/weather_files/'+str(date)+'/'
 
            if not os.path.exists(path):
                os.makedirs(path)
            for file in WeatherDateStat:
                cache = open(path+file.split('/')[-1],'wb')
                # print file
                try:
                    ftp.retrbinary("RETR " + file, cache.write, 8*1024)
                except:
                    ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
                    # 'Succesfully Connected...'
                    ftp.login()
                    ftp.retrbinary("RETR " + file, cache.write, 8*1024)
                   
                cache.close()
                adapter = iopro.text_adapter(cache.name,compression='gzip',parser='csv', field_names=True)
                avg_temp = avg_temp + list(adapter[:]['TEMP'])
                # mean = (mean+adapter[:]['TEMP'].mean())/2.0
                # stdev = np.sqrt(stdev**2+adapter[:]['TEMP'].std()**2)/2.0
                adapter.close()

            print 'Date Mean Std: ', date, np.mean(avg_temp), np.std(avg_temp)
            out.add(date, (np.mean(avg_temp),np.std(avg_temp)))
            # out.add(date, (mean,stdev))
  
 
 
 
if __name__ == "__main__":
    import ftplib,os
    import tempfile
 
    from WeatherDisco import Filter1973
    from WeatherDisco import ChainStations
    from WeatherDisco import PersistingYears
    from WeatherDisco import StationSets
    from WeatherDisco import CoverageSets
    from WeatherDisco import StationSetsCoverage
    from WeatherDisco import CalculateAverages
    
    import sys, iopro
    
    filePath = '/tmp/'
    out = open(filePath+'global_averages.csv','w')
    
    filter1973 = Filter1973().run()
    collectedStations = ChainStations().run(input=filter1973.wait(show=False))
    persistingYears = PersistingYears().run(input=collectedStations.wait(show=False))
    stationSet = StationSets().run(input=persistingYears.wait(show=False))
    coverage = CoverageSets().run(input=stationSet.wait(show=True))
    stationSetsCoverage = StationSetsCoverage().run(input=coverage.wait(show=True))
    averages = CalculateAverages().run(input=stationSetsCoverage.wait(show=True))
    
    
    for date, (avg, std) in result_iterator(averages.wait(show=True)):
        print date, avg,std
        print >> out, date, '\t', avg,'\t', std
           import ftplib,os
 
        ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
        ftp.login()
 
        path = '/tmp/weather_files_1973/'
        if not os.path.exists(path):
            os.makedirs(path)
 
        stations = []
 
        #download file station report
        for stat1973, v in stationIDs:
            cache = open(path+stat1973.split('/')[-1],'wb')
 
            try:
                ftp.retrbinary("RETR " + stat1973, cache.write, 8*1024)
            except:
                ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
                # 'Succesfully Connected...'
                ftp.login()
                ftp.retrbinary("RETR " + stat1973, cache.write, 8*1024)
             
            cache.close()
 
            #skip tar file
            if stat1973.endswith('.op.gz'):
                adapter = iopro.text_adapter(cache.name,compression='gzip',parser='csv', field_names=True)
 
                avg_temp = adapter[:]['TEMP']
                
                if (len(avg_temp) < 360) or (len(avg_temp) > 366):
                    # print '\ttoo small or too big'
                    continue      
                if 9999.9 in avg_temp:
                    continue
                else:
                    stations.append(cache.name.split('/')[-1][:12])
                    #store station which has good coverage for the year
 
        out.add(1,stations)
        #yield list of stations
 
        ftp.close()
 
class ChainStations(Job):
    params = 4
    map_reader = staticmethod(chain_reader)
 
    map = staticmethod(nop_map)
 
    @staticmethod
    def reduce(stationIDs, out, params):
        stations = []
        for v,statID in stationIDs:
             stations = stations+statID
 
        stations = set(stations)
        ranges = []
        step = (2013-1974)/int(params)
        for i in range(int(params)):
            if i == int(params)-1:
                ranges.append(range(1974+i*step,2013))
            else:
                ranges.append(range(1974+i*step,1974+(i+1)*step))

        
        for i,r in enumerate(ranges):
            out.add(i,(stations,r))
        
class PersistingYears(Job):
    partitions = 4
    map_reader = staticmethod(chain_reader)
    map = staticmethod(nop_map)
 
    @staticmethod
    def reduce(iter,out, params):
        import ftplib,os
 
        ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
        ftp.login()
 
        for key, StatRange in iter:
            stations = set(StatRange[0])
            ranges = StatRange[1]
            
            for date in ranges:
                try:
                    files = ftp.nlst('pub/data/gsod/'+str(date))
                except:
                    ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
                    # 'Succesfully Connected...'
                    ftp.login()
                    files = ftp.nlst('pub/data/gsod/'+str(date))
                 
                files = [f.split('/')[-1][:12] for f in files]
 
                stations = set(stations) & set(files)

        out.add(key,stations)
 
class StationSets(Job):
    params = 4
    map_reader = staticmethod(chain_reader)
 
    map = staticmethod(nop_map)
 
    @staticmethod
    def reduce(stationIDs, out, params):
        stationSet = set()
        for v,statID in stationIDs:
            if not stationSet:
                stationSet = stationSet.union(statID)
            stationSet = stationSet & statID
            # for stid in statID:
            #     if stid not in stationSet:
            #         stationSet.add(stid) 
        stationList = list(stationSet)
        l = len(stationList)
        
        fileout = open('/tmp/set_of_stations.csv','w')
    
        for stat in stationList:
            print >> fileout, stat

        ranges = []
        step = (2013-1974)/int(params)
        for i in range(int(params)):
            if i == int(params)-1:
                ranges.append(range(1974+i*step,2013))
            else:
                ranges.append(range(1974+i*step,1974+(i+1)*step))

        
        for i,r in enumerate(ranges):
            out.add(i,(stationList,r))

        # if l%4 == 0:
        #     for i in range(4):
        #         out.add(i,stationList[0+i*l/4:l/4*(i+1)])
        # else:
        #     for i in range(4):
        #         if i == 3:
        #             out.add(i,stationList[0+i*l/4:l])
        #         else:
        #             out.add(i,stationList[0+i*l/4:l/4*(i+1)])

class CoverageSets(Job):
    partitions = 4
    map_reader = staticmethod(chain_reader)
    map = staticmethod(nop_map)
 
    @staticmethod
    def reduce(iter,out, params):
        import ftplib,os
 
        ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
        ftp.login()

        gsod_path = 'pub/data/gsod/'
        dirpath = '/tmp/weather_files_coverage/'

        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        
        for key, StatRange in iter:
            stations = list(StatRange[0])
            ranges = StatRange[1]
            
            for date in ranges:
                for stat in stations:
                    
                    cache = open(dirpath+stat+'-'+str(date)+'.op.gz','wb')
                    f = gsod_path+str(date)+'/'+stat+'-'+str(date)+'.op.gz'
                   
                    try:
                        ftp.retrbinary("RETR " + f, cache.write, 8*1024)
                    except:
                        ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
                        # 'Succesfully Connected...'
                        ftp.login()
                        ftp.retrbinary("RETR " + f, cache.write, 8*1024)

                    try:
                        cache.close()
                        adapter = iopro.text_adapter(cache.name,compression='gzip',parser='csv', field_names=True)
                        avg_temp = adapter[:]['TEMP']
                    except:
                        stations.remove(stat)
                        continue                    
                    if (len(avg_temp) < 360):
                        stations.remove(stat)
                        continue       
                    if 9999.9 in avg_temp:
                        stations.remove(stat)
                        continue
                
        out.add(1,set(stations))
            
class StationSetsCoverage(Job):
    params = 4
    map_reader = staticmethod(chain_reader)
 
    map = staticmethod(nop_map)
 
    @staticmethod
    def reduce(stationIDs, out, params):
        stationSet = set()
        for v,statID in stationIDs:
            if not stationSet:
                stationSet = stationSet.union(statID)
            stationSet = stationSet & statID
            # for stid in statID:
            #     if stid not in stationSet:
            #         stationSet.add(stid) 
        stationList = list(stationSet)
        l = len(stationList)
        
        fileout = open('/tmp/set_of_stations-fullcoverage.csv','w')
    
        for stat in stationList:
            print >> fileout, stat

        
        if l%4 == 0:
            for i in range(4):
                out.add(i,stationList[0+i*l/4:l/4*(i+1)])
        else:
            for i in range(4):
                if i == 3:
                    out.add(i,stationList[0+i*l/4:l])
                else:
                    out.add(i,stationList[0+i*l/4:l/4*(i+1)])


class CalculateAverages(Job):
    partitions = 4
    sort = True
    map_reader = staticmethod(chain_reader)
    
    @staticmethod
    def map(StationSet, params):
        path = 'pub/data/gsod/'
        
        k,stats = StationSet
        
        for i, stid in enumerate(stats):
            for date in range(1973,2013):
                WeatherDateStat = path+str(date)+'/'+str(stid)+'-'+str(date)+'.op.gz'
                yield (str(date), WeatherDateStat)
 
    @staticmethod
    def reduce(iter, out, params):
        import numpy as np
        import ftplib,os
        import iopro,shutil
        from disco.util import kvgroup
        
        for date, WeatherDateStat in kvgroup(iter):
            print date
            # print 'Connecting to NOAA...'
            ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
            # print 'Succesfully Connected...'
            ftp.login()
 
            avg_temp = []
            
            stdev = 0
            SUM = 0
            mean = 0

            path = '/tmp/weather_files/'+str(date)+'/'
 
            if not os.path.exists(path):
                os.makedirs(path)
            for file in WeatherDateStat:
                cache = open(path+file.split('/')[-1],'wb')
                # print file
                try:
                    ftp.retrbinary("RETR " + file, cache.write, 8*1024)
                except:
                    ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
                    # 'Succesfully Connected...'
                    ftp.login()
                    ftp.retrbinary("RETR " + file, cache.write, 8*1024)
                   
                cache.close()
                adapter = iopro.text_adapter(cache.name,compression='gzip',parser='csv', field_names=True)
                avg_temp = avg_temp + list(adapter[:]['TEMP'])
                # mean = (mean+adapter[:]['TEMP'].mean())/2.0
                # stdev = np.sqrt(stdev**2+adapter[:]['TEMP'].std()**2)/2.0
                adapter.close()

            print 'Date Mean Std: ', date, np.mean(avg_temp), np.std(avg_temp)
            out.add(date, (np.mean(avg_temp),np.std(avg_temp)))
            # out.add(date, (mean,stdev))
  
 
 
 
if __name__ == "__main__":
    import ftplib,os
    import tempfile
 
    from WeatherDisco import Filter1973
    from WeatherDisco import ChainStations
    from WeatherDisco import PersistingYears
    from WeatherDisco import StationSets
    from WeatherDisco import CoverageSets
    from WeatherDisco import StationSetsCoverage
    from WeatherDisco import CalculateAverages
    
    import sys, iopro
    
    filePath = '/tmp/'
    out = open(filePath+'global_averages.csv','w')
    
    filter1973 = Filter1973().run()
    collectedStations = ChainStations().run(input=filter1973.wait(show=False))
    persistingYears = PersistingYears().run(input=collectedStations.wait(show=False))
    stationSet = StationSets().run(input=persistingYears.wait(show=False))
    coverage = CoverageSets().run(input=stationSet.wait(show=True))
    stationSetsCoverage = StationSetsCoverage().run(input=coverage.wait(show=True))
    averages = CalculateAverages().run(input=stationSetsCoverage.wait(show=True))
    
    
    for date, (avg, std) in result_iterator(averages.wait(show=True)):
        print date, avg,std
        print >> out, date, '\t', avg,'\t', std
    