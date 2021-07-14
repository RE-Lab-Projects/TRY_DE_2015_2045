# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd
import glob
import os

# %%
# Read and format raw data and save to csv
# Pattern for the dataset names: TRY_[1-15]_[a,w,s]_[2015,2045] with
# 1-15  -> test reference region number
# a,w,s -> average, extreme winter, extreme summer
root=os.getcwd()
year=['2015','2045']
station = ['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15']
mode = ['Jahr','Somm','Wint']

for y in year:
    for s in station:
        for m in mode:
            fn1=glob.glob(root+'/1_raw-data/'+s+'/*'+y+'*'+m+'.dat')
            fn2=glob.glob(root+'/2_synthetic-radiation/'+s+'/*'+y+'*'+m+'*'+'.dat')
            for f1, f2 in zip(fn1, fn2):
                # change notation
                if m=='Jahr':
                    m='a'
                elif m=='Somm':
                    m='s'
                else:
                    m='w'

                # skiprows 34 / 36 if year ist 2015 / 2045
                if y=='2015':
                    sr=34
                    y1='2014'
                    y2='2015'
                else:
                    sr=36
                    y1='2044'
                    y2='2045'
                # load DWD data file
                vars()['TRY'+'_'+s+'_'+m+'_'+y] = pd.read_csv(f1, sep='\s+', skiprows=sr, header=None, usecols=[5,6,7,8,9,11,12,13])
                # rename columns
                vars()['TRY'+'_'+s+'_'+m+'_'+y].rename(columns={5:'temperature [degC]', 
                                6:'pressure [hPa]',
                                7:'wind direction [deg]',
                                8:'wind speed [m/s]',
                                9:'cloud coverage [1/8]',
                                11:'humidity [%]',
                                12:'direct irradiance [W/m^2]',
                                13:'diffuse irradiance [W/m^2]'}, inplace=True)
                # load synthetic data file
                data_synth = pd.read_csv(f2, sep=',', skiprows=1, header=None)
                # rename columns
                data_synth.rename(columns={0:'synthetic global irradiance [W/m^2]', 
                                1:'synthetic diffuse irradiance [W/m^2]',
                                2:'clear sky irradiance [W/m^2]'},
                                inplace=True)
                # generate datetime column
                vars()['TRY'+'_'+s+'_'+m+'_'+y]['datetime'] = pd.date_range(start=y+'-01-01 00:00:00', end=y+'-12-31 23:00:00', freq='h', tz='CET')
                # set datetime as index
                vars()['TRY'+'_'+s+'_'+m+'_'+y].set_index(keys='datetime',inplace=True)
                # save to csv
                vars()['TRY'+'_'+s+'_'+m+'_'+y].to_csv(root+'/3_processed-data/TRY'+'_'+s+'_'+m+'_'+y+'_60min.csv')
                # upsampling to 1min temporal resolution and linear interpolation
                vars()['TRY'+'_'+s+'_'+m+'_'+y].index = pd.to_datetime(vars()['TRY'+'_'+s+'_'+m+'_'+y].index, utc=True)
                vars()['TRY'+'_'+s+'_'+m+'_'+y].index = vars()['TRY'+'_'+s+'_'+m+'_'+y].index.shift(1800, freq='s')
                vars()['TRY'+'_'+s+'_'+m+'_'+y].loc[pd.to_datetime(y1+'-12-31 22:30:00+00:00')] = vars()['TRY'+'_'+s+'_'+m+'_'+y].iloc[0].values
                vars()['TRY'+'_'+s+'_'+m+'_'+y] = vars()['TRY'+'_'+s+'_'+m+'_'+y].sort_index()
                vars()['TRY'+'_'+s+'_'+m+'_'+y].loc[pd.to_datetime(y2+'-12-31 23:30:00+00:00')] = vars()['TRY'+'_'+s+'_'+m+'_'+y].iloc[8760].values
                data = vars()['TRY'+'_'+s+'_'+m+'_'+y].tz_convert('CET')
                vars()['TRY'+'_'+s+'_'+m+'_'+y+'_1min'] = data.resample('1min').interpolate().round(1)
                vars()['TRY'+'_'+s+'_'+m+'_'+y+'_1min'] = vars()['TRY'+'_'+s+'_'+m+'_'+y+'_1min'].iloc[30:-31]
                # add columns with synthetic radiation from Hofmann et. al. model
                data_synth['datetime'] = vars()['TRY'+'_'+s+'_'+m+'_'+y+'_1min'].index
                data_synth.set_index(keys='datetime',inplace=True)
                vars()['TRY'+'_'+s+'_'+m+'_'+y+'_1min'] = pd.concat([vars()['TRY'+'_'+s+'_'+m+'_'+y+'_1min'], data_synth], axis=1)
                # save to csv
                vars()['TRY'+'_'+s+'_'+m+'_'+y+'_1min'].to_csv(root+'/3_processed-data/TRY'+'_'+s+'_'+m+'_'+y+'_1min.csv')
                # downsampling to 15min temporal resolution
                vars()['TRY'+'_'+s+'_'+m+'_'+y+'_15min'] = vars()['TRY'+'_'+s+'_'+m+'_'+y+'_1min'].resample('15T').mean().round(1)
                # save to csv
                vars()['TRY'+'_'+s+'_'+m+'_'+y+'_15min'].to_csv(root+'/3_processed-data/TRY'+'_'+s+'_'+m+'_'+y+'_15min.csv')
                # delete from workspace
                del vars()['TRY'+'_'+s+'_'+m+'_'+y], vars()['TRY'+'_'+s+'_'+m+'_'+y+'_1min'], vars()['TRY'+'_'+s+'_'+m+'_'+y+'_15min'], data, data_synth

# %%
