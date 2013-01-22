import iopro
import sys
import pandas as pd

df = pd.read_csv('station_complete.csv',header=False)
df.columns=['STDIN']


stations = pd.read_csv('ish-history.csv')
adapter = iopro.text_adapter('station_complete.csv',parser='csv',delimiter=' ',field_names=False)
usaf_list = list(adapter[:]['f0'])

station_clean = stations['USAF'].map(lambda x: x in usaf_list)
cleaned = stations[station_clean]

lat = cleaned['LAT']
lon = cleaned['LON']


import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap

from mpl_toolkits.basemap import Basemap
import numpy as np
import matplotlib.pyplot as plt

m = Basemap(projection='merc',llcrnrlat=-80,urcrnrlat=80,\
            llcrnrlon=-180,urcrnrlon=180,lat_ts=20,resolution='c')
m.drawcoastlines()
m.fillcontinents(color='coral',lake_color='aqua')

# draw parallels and meridians.
m.drawparallels(np.arange(-90.,91.,30.))
m.drawmeridians(np.arange(-180.,181.,60.))
m.drawmapboundary(fill_color='aqua')

m.drawcountries()
m.drawcoastlines()
m.drawstates()

#lon, lat will convert lon/lat (in degrees) to x/y map projection coordinates (in meters). 
x, y = m(lon.values/1000.0, lat.values/1000.0)
m.plot(x, y, 'g.', alpha=0.9)

plt.title("Station Location Map")
plt.show()
