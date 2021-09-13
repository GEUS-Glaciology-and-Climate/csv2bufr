#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep  9 13:44:49 2021

@author: pho

Playground script for converting PROMICE .txt files to WMO-compliant BUFR files

This script uses the package eccodes to run. 
https://confluence.ecmwf.int/display/ECC/ecCodes+installation
Eccodes is the official package for WMO BUFR file construction. Eccodes must 
be configured on your computer BEFORE downloading the eccodes python bindings.
Eccodes can be configured with the conda python bindings, using the command
'conda install eccodes', however this didn't seem to work for me. Instead, I 
built eccodes separately and then installed the python bindings using pip3.
 
See here for a step-by-step guide on the eccodes set-up:
https://gist.github.com/MHBalsmeier/a01ad4e07ecf467c90fad2ac7719844a

Processing steps based on this example:
https://confluence.ecmwf.int/display/UDOC/How+do+I+create+BUFR+from+a+CSV+-+ecCodes+BUFR+FAQ
    
"""
import pandas as pd
# import xarray as xr
import glob
from eccodes import codes_set, codes_write, codes_release, codes_bufr_new_from_samples, CodesInternalError
# from pybufrkit.encoder import Encoder

#------------------------------------------------------------------------------

def getTXT(filename, delim='\s+'):
    '''Get values from .txt or .csv file
    
    Variables
    filename (str)          File path to .txt or .csv file
    
    Returns
    df (DataFrame)          Pandas dataframe of imported values'''

    df = pd.read_csv(filename, delimiter=delim)
    return df


def setBUFRvalue(ibufr, b_name, value):
    '''Set variable in BUFR message
    
    Variables
    ibufr ()                Active BUFR message 
    b_name (str)            BUFR message variable name
    value (int/float)       Value to be assigned to variable
    '''
    try:
        codes_set(ibufr, b_name, value)
    except:
        pass
    

def getBUFR(df1, df2, outBUFR, ed=4, master=0, vers=31, template=307092, key='unexpandedDescriptors'):
    '''Construct and export .bufr messages to file from DataFrame.
    
    Variables
    df (DataFrame)          Pandas dataframe of weather station observations
    df2 (DataFrame)         Pandas dataframe of lookup table
    outBUFR (str)           File path that .bufr file will be exported to
    ed (int)                BUFR table edition (default=4)
    master (int)            Master table number (default=0)
    vers (int)              Master table version number (default=31)
    template (int)          Template table number (default=307092)
    key (str)               Encoding key name (default="unexpandedDescriptors")
    
    Returns 
    None
    '''

    #Open bufr file
    fout = open(outBUFR, 'wb')
    
    #Iterate over rows in weather observations dataframe
    for i1, r1 in df1.iterrows():

        #Create new bufr message to write to
        ibufr = codes_bufr_new_from_samples('BUFR4')  
        
        try:
            #Indicator section (BUFR 4 letters, total msg size, edition number)                             
            codes_set(ibufr, 'edition', ed)                                    #Currently edition 4 
           
            #Identification section (master table, id, sequence number, data cat number)
            codes_set(ibufr, 'masterTableNumber', master)                      #0 = standard WMO FM 94 BUFR tables
            codes_set(ibufr, 'masterTablesVersionNumber', vers)                #Table version
            codes_set(ibufr, 'localTablesVersionNumber', 0)
    
            codes_set(ibufr, 'bufrHeaderCentre', 98)                           #98 = centre is ecmf
            codes_set(ibufr, 'bufrHeaderSubCentre', 0)
            codes_set(ibufr, 'updateSequenceNumber', 0)
            codes_set(ibufr, 'dataCategory', 0)                                #0 = Surface data, land
            codes_set(ibufr, 'internationalDataSubCategory', 7)                #7 = n-min obs from AWS stations
            codes_set(ibufr, 'dataSubCategory', 7)
    
            codes_set(ibufr, 'observedData', 1)
            codes_set(ibufr, 'compressedData', 0)
            codes_set(ibufr, 'typicalYear', int(r1['Year']))
            codes_set(ibufr, 'typicalMonth', int(r1['MonthOfYear']))
            codes_set(ibufr, 'typicalDay', int(r1['DayOfMonth']))
            codes_set(ibufr, 'typicalHour', int(r1['HourOfDay(UTC)']))
            codes_set(ibufr, 'typicalMinute', 0)
            codes_set(ibufr, 'typicalSecond', 0)    
            
            #Assign message template
            ivalues = (template)                                               #307091 =  surfaceObservationOneHour

            #Assign key name to encode sequence number                             
            codes_set(ibufr, key, ivalues) 
                  
            #Data Description and Binary Data section
            for i2, r2 in df2.iterrows():
                
                #Write value only if lookup table variable name is present
                if pd.isnull(r2['standard_name']) is False:
                    
                    #Assign value based on type defined in lookup table
                    if str(r2['type']) in 'int':
                        codes_set(ibufr, r2['standard_name'], int(r1[r2['CSV_column']]))
                    elif str(r2['type']) in 'float':
                        codes_set(ibufr, r2['standard_name'], float(r1[r2['CSV_column']]))                    
                    elif str(r2['type']) in 'str':
                        codes_set(ibufr, r2['standard_name'], str(r1[r2['CSV_column']]))                      
                               
            # codes_set(ibufr, 'year', int(r1['Year']))
            # codes_set(ibufr, 'month', int(r1['MonthOfYear']))
            # codes_set(ibufr, 'day', int(r1['DayOfMonth']))
            # codes_set(ibufr, 'hour', int(r1['HourOfDay(UTC)']))
            # # codes_set(ibufr, 'minute', int(r1[4]))
            # # codes_set(ibufr, 'blockNumber', int(r1[5]))
            # # codes_set(ibufr, 'stationNumber', int(r1[6]))
            # # codes_set(ibufr, 'longStationName',r1[7].strip())
            # codes_set(ibufr, 'latitude', float(r1['LatitudeGPS(degN)']))
            # codes_set(ibufr, 'longitude', float(r1['LongitudeGPS(degW)']))
            # codes_set(ibufr, 'heightOfStationGroundAboveMeanSeaLevel', float(r1['ElevationGPS(m)']))
            # codes_set(ibufr, 'pressure', float(r1['AirPressure(hPa)']))
            # # codes_set(ibufr, 'pressureReducedToMeanSeaLevel', float(r1[12]))
            # codes_set(ibufr, 'airTemperature', float(r1['AirTemperature(C)']))
            # codes_set(ibufr, 'relativeHumidity', float(r1['RelativeHumidity(%)']))
            # # codes_set(ibufr, '#2#timePeriod', -10)                                           # -10: Period of precipitation observation is 10 minutes
            # # codes_set(ibufr, 'totalPrecipitationOrTotalWaterEquivalent', float(r1[15]))
            # # codes_set(ibufr, '#1#timeSignificance', 2)                                       # 2: Time averaged
            # # codes_set(ibufr, '#3#timePeriod', -10)                                           # -10: Period of wind observations is 10 minutes
            # codes_set(ibufr, 'windDirection', float(r1['WindDirection(d)']))
            # codes_set(ibufr, 'windSpeed', float(r1['WindSpeed(m/s)']))   
 
            #Encode keys in data section
            codes_set(ibufr, 'pack', 1)                                            
            
            #Write bufr message to bufr file
            codes_write(ibufr, fout)

        except CodesInternalError as ec:
            print(ec)
            
        codes_release(ibufr)            
        
    fout.close()
   

#------------------------------------------------------------------------------

if __name__ == '__main__':
 
    #Get all txt files in directory
    txtFiles = glob.glob ('./*hour*')
    
    #Get lookup table
    lookup = getTXT('./variables_bufr.csv', None)
       
    #Iterate through txt files
    for fname in txtFiles:
    
        #Generate output BUFR filename
        bufrname = fname.split('/')[-1].split('.txt')[0][:-4][:-5]+'.bufr'
        print(f'Generating {bufrname} from {fname}')
    
        #Get text file
        df1 = getTXT(fname)
        
        #Construct and export BUFR file
        getBUFR(df1, lookup, bufrname)
        print(f'Successfully export bufr file to {bufrname}')   
        
    print('Finished')
