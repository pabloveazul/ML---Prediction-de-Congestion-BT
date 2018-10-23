
# coding: utf-8

# In[1]:


# Import all libraries needed for the tutorial
# General syntax to import specific functions in a library: 
##from (library) import (specific library function)
from pandas import DataFrame, read_csv
# General syntax to import a library but no functions: 
##import (library) as (give the library a nickname/alias)
import matplotlib.pyplot as plt
import pandas as pd #this is how I usually import pandas
import sys #only needed to determine Python version number
import matplotlib #only needed to determine Matplotlib version number
import functools
from spectrum import *
from pylab import plot
from scipy import signal
import numpy as np
import time
from scipy import stats
# Enable inline plotting


# In[2]:


print('Python version ' + sys.version)
print('Pandas version ' + pd.__version__)
print('Matplotlib version ' + matplotlib.__version__)


# In[ ]:


from datetime import timedelta, date, datetime
def KSTestPerDay(cell1,cell2,kpi,ZoneDataFrame):
    cell1_kpi = ZoneDataFrame.loc[ZoneDataFrame['CellName']==cell1][['DAY_',kpi]]
    cell1_kpi['DAY_'] = pd.to_datetime(cell1_kpi['DAY_'],infer_datetime_format=True)
    
    cell2_kpi = ZoneDataFrame.loc[ZoneDataFrame['CellName']==cell2][['DAY_',kpi]]
    cell2_kpi['DAY_'] = pd.to_datetime(cell2_kpi['DAY_'],infer_datetime_format=True)
    KS = pd.DataFrame()
    for day in np.array([datetime(2018,8,7)+timedelta(days=i) for i in range(1,6)]):
        dayvalue = pd.DataFrame()
        dayvalue['Day'] = [day]
        mask1 = (cell1_kpi['DAY_'] >=day) & (cell1_kpi['DAY_'] <day+timedelta(days=1))
        mask2 = (cell2_kpi['DAY_'] >=day) & (cell2_kpi['DAY_'] <day+timedelta(days=1))

        dayvalue['pvalue'] = [stats.ks_2samp(cell1_kpi.loc[mask1][kpi],cell2_kpi.loc[mask2][kpi])[1]]
        KS = KS.append(dayvalue,ignore_index = True)
    return(KS)


# In[ ]:


from datetime import timedelta, date, datetime
def KSTestPerWeekDay(cell1,cell2,kpi,ZoneDataFrame):
    cell1_kpi = ZoneDataFrame.loc[ZoneDataFrame['CellName']==cell1][['DAY_',kpi]]   
    cell1_kpi['DAY_'] = pd.to_datetime(cell1_kpi['DAY_'],infer_datetime_format=True)
    cell2_kpi = ZoneDataFrame.loc[ZoneDataFrame['CellName']==cell2][['DAY_',kpi]]
    cell2_kpi['DAY_'] = pd.to_datetime(cell2_kpi['DAY_'],infer_datetime_format=True)
  
    KS = pd.DataFrame()
    for day in range(0,7):
        dayvalue = pd.DataFrame()
        dayvalue['Day'] = [day]
        mask1 = (cell1_kpi['DAY_'].dt.dayofweek==day)
        mask2 = (cell2_kpi['DAY_'].dt.dayofweek==day)
        dayvalue['pvalue'] = [stats.ks_2samp(cell1_kpi.loc[mask1][kpi],cell2_kpi.loc[mask2][kpi])[1]]
        KS = KS.append(dayvalue,ignore_index = True)
    return(KS)


# In[16]:


def KSTestPerDayAverageParallelFunc(ZoneDataFrame,cell1,cell2,kpi):
    KS = pd.DataFrame()
    if cell1 == cell2:
        KS.at[str(cell1),str(cell2)] = float(1)
    else:
        avg = KSTestPerDay(cell1,cell2,kpi,ZoneDataFrame)['pvalue'].mean()
        KS.at[cell1,cell2] = avg
    return(KS)


# In[15]:


def KSTestPerWeekDayAverageParallelFunc(ZoneDataFrame,cell1,cell2,kpi):
    KS = pd.DataFrame()
    if cell1 == cell2:
        KS.at[str(cell1),str(cell2)] = float(1)
    else:
        avg = KSTestPerWeekDay(cell1,cell2,kpi,ZoneDataFrame)['pvalue'].mean()
        KS.at[cell1,cell2] = avg
    return(KS)


# In[12]:


from datetime import timedelta, date, datetime
def KSTestPerWeekDayParallelFunc(cell1,cell2,kpi,day, ZoneDataFrame):
    cell1_kpi = ZoneDataFrame.loc[ZoneDataFrame['CellName']==cell1][['DAY_',kpi]]   
    cell1_kpi['DAY_'] = pd.to_datetime(cell1_kpi['DAY_'],infer_datetime_format=True)
    cell2_kpi = ZoneDataFrame.loc[ZoneDataFrame['CellName']==cell2][['DAY_',kpi]]
    cell2_kpi['DAY_'] = pd.to_datetime(cell2_kpi['DAY_'],infer_datetime_format=True)
  
    KS = pd.DataFrame()
    dayvalue = pd.DataFrame()
    dayvalue['Day'] = [day]
    mask1 = (cell1_kpi['DAY_'].dt.dayofweek==day)
    mask2 = (cell2_kpi['DAY_'].dt.dayofweek==day)
    dayvalue['pvalue'] = [stats.ks_2samp(cell1_kpi.loc[mask1][kpi],cell2_kpi.loc[mask2][kpi])[1]]
    KS = KS.append(dayvalue,ignore_index = True)
    return(KS)


# In[14]:


from datetime import timedelta, date, datetime
def KSTestPerDayParallelFunc(cell1,cell2,kpi,day,ZoneDataFrame):
    cell1_kpi = ZoneDataFrame.loc[ZoneDataFrame['CellName']==cell1][['DAY_',kpi]]
    cell1_kpi['DAY_'] = pd.to_datetime(cell1_kpi['DAY_'],infer_datetime_format=True)
    
    cell2_kpi = ZoneDataFrame.loc[ZoneDataFrame['CellName']==cell2][['DAY_',kpi]]
    cell2_kpi['DAY_'] = pd.to_datetime(cell2_kpi['DAY_'],infer_datetime_format=True)
    KS = pd.DataFrame()
    dayvalue = pd.DataFrame()
    dayvalue['Day'] = [day]
    mask1 = (cell1_kpi['DAY_'] >=day) & (cell1_kpi['DAY_'] <day+timedelta(days=1))
    mask2 = (cell2_kpi['DAY_'] >=day) & (cell2_kpi['DAY_'] <day+timedelta(days=1))

    dayvalue['pvalue'] = [stats.ks_2samp(cell1_kpi.loc[mask1][kpi],cell2_kpi.loc[mask2][kpi])[1]]
    KS = KS.append(dayvalue,ignore_index = True)
    return(KS)

def KSTestParallelFunc(ZoneDataFrame,cell1,cell2,kpiList,dayList, wh = True):
    auxList = [kpi for kpi in kpiList]
    auxList.append('DAY_')
    cell1_kpi = ZoneDataFrame.loc[ZoneDataFrame['CellName']==cell1][auxList]
    cell1_kpi['DAY_'] = pd.to_datetime(cell1_kpi['DAY_'],infer_datetime_format=True)
    cell1_kpi = cell1_kpi.set_index('DAY_')
    cell2_kpi = ZoneDataFrame.loc[ZoneDataFrame['CellName']==cell2][auxList]
    cell2_kpi['DAY_'] = pd.to_datetime(cell2_kpi['DAY_'],infer_datetime_format=True)
    cell2_kpi = cell2_kpi.set_index('DAY_')
    out = {}
    if wh:
        cell1_kpi = cell1_kpi.between_time('7:00','23:00')
        cell1_kpi['DAY_'] = cell1_kpi.index
        cell1_kpi['DAY_'] = pd.to_datetime(cell1_kpi['DAY_'],infer_datetime_format=True)
        cell2_kpi = cell2_kpi.between_time('7:00','23:00')
        cell2_kpi['DAY_'] = cell2_kpi.index
        cell2_kpi['DAY_'] = pd.to_datetime(cell2_kpi['DAY_'],infer_datetime_format=True)
    for day in dayList:
        out[day] = {}
        mask1 = (cell1_kpi['DAY_'] >=day) & (cell1_kpi['DAY_'] <day+timedelta(days=1) ) 
        mask2 = (cell2_kpi['DAY_'] >=day) & (cell2_kpi['DAY_'] <day+timedelta(days=1) )
        for kpi in kpiList:
            pvalue = [stats.ks_2samp(cell1_kpi.loc[mask1][kpi],cell2_kpi.loc[mask2][kpi])[1]]
            out[day][kpi] = pvalue
    return(out)
