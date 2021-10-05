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
   

According to DMI, the BUFR messages should adhere to Common Code Table 13:
https://confluence.ecmwf.int/display/ECC/WMO%3D13+element+table#WMO=13elementtable-CL_1
"""
import pandas as pd
import glob, os
from eccodes import codes_set, codes_write, codes_release, \
                    codes_bufr_new_from_samples, CodesInternalError
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


def setBUFRvalue(ibufr, b_name, value, nullvalue=-999):
    '''Set variable in BUFR message
    
    Variables
    ibufr ()                    Active BUFR message 
    b_name (str)                BUFR message variable name
    value (int/float)           Value to be assigned to variable
    nullvalue (int/float)       Null value to be assigned to variable
    '''
    nullFlag=False
    if isinstance(value, int) or isinstance(value, float):
        if value==nullvalue:
            nullFlag=True
            # print('Null value found in ' + str(b_name))
    if nullFlag==False:
        try:
            codes_set(ibufr, b_name, value)
        except CodesInternalError as ec:
            print(f'{ec}: {b_name}')


def getTempK(row, nullvalue=-999):
    '''Convert temperature from celsius to kelvin units    
    '''
    if row['AirTemperature(C)'] == nullvalue:
        return nullvalue
    else:
        return row['AirTemperature(C)'] + 273.15


def getPressPa(row, nullvalue=-999):
    '''Convert hPa pressure values to Pa units   
    '''
    if row['AirPressure(hPa)'] == nullvalue:
        return nullvalue
    else:
        return row['AirPressure(hPa)']*100

    

def getBUFR(df1, df2, outBUFR, ed=4, master=0, vers=13, 
            template=307080, key='unexpandedDescriptors'):
    '''Construct and export .bufr messages to file from DataFrame.
    
    Variables
    df (DataFrame)          Pandas dataframe of weather station observations
    df2 (DataFrame)         Pandas dataframe of lookup table
    outBUFR (str)           File path that .bufr file will be exported to
    ed (int)                BUFR table edition (default=4)
    master (int)            Master table number (default=0, standard WMO FM 94
                            BUFR tables)
    vers (int)              Master table version number (default=13)
    template (int)          Template table number (default=307080)
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
            #Current edition is version 4                             
            codes_set(ibufr, 'edition', ed)                                    
           
            #Identification section (master table, id, sequence number, data cat number)
            codes_set(ibufr, 'masterTableNumber', master)                      
            codes_set(ibufr, 'masterTablesVersionNumber', vers)                
            codes_set(ibufr, 'localTablesVersionNumber', 0)
            
            #BUFR header centre 98 = ECMF
            codes_set(ibufr, 'bufrHeaderCentre', 98)                           
            codes_set(ibufr, 'bufrHeaderSubCentre', 0)
            codes_set(ibufr, 'updateSequenceNumber', 0)
            
            #Data category 0 = surface data, land
            codes_set(ibufr, 'dataCategory', 0)    

            #International data subcategory 7 = n-min obs from AWS stations                
            codes_set(ibufr, 'internationalDataSubCategory', 7)                
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
            ivalues = (template)
            
            #Assign key name to encode sequence number                             
            codes_set(ibufr, key, ivalues) 
            
            #Data Description and Binary Data section
            #Set AWS station info
            
            #Need to set WMO block and station number
            codes_set(ibufr, 'stationNumber', 1)
            codes_set(ibufr, 'blockNumber', 1)
            # codes_set(ibufr, 'wmoRegionSubArea', 1)
            
            # #Region number=7 (unknown)
            # codes_set(ibufr, 'regionNumber', 7)
            
            #Unset parameters
            # codes_set(ibufr, 'stationOrSiteName', CCITT IA5)
            # codes_set(ibufr, 'shortStationName', CCITT IA5)
            # codes_set(ibufr, 'shipOrMobileLandStationIdentifier', CCITT IA5)
            # codes_set(ibufr, 'directionOfMotionOfMovingObservingPlatform', deg)
            # codes_set(ibufr, 'movingObservingPlatformSpeed', m/s)
            
            codes_set(ibufr, 'stationType', 0)
            codes_set(ibufr, 'instrumentationForWindMeasurement', 6)
            # codes_set(ibufr, 'measuringEquipmentType', 0)
            # codes_set(ibufr, 'temperatureObservationPrecision', 0.1)
            # codes_set(ibufr, 'solarAndInfraredRadiationCorrection', 0)
            # codes_set(ibufr, 'pressureSensorType', 30)
            # codes_set(ibufr, 'temperatureSensorType', 30) 
            # codes_set(ibufr, 'humiditySensorType', 30) 
            
            
            #Set AWS variables
            for i2, r2 in df2.iterrows():
                
                #Write value only if lookup table variable name is present
                if pd.isnull(r2['standard_name']) is False:
                    
                    #Assign value based on type defined in lookup table
                    if str(r2['type']) in 'int':
                        setBUFRvalue(ibufr, r2['standard_name'], int(r1[r2['CSV_column']]))   
                    elif str(r2['type']) in 'float':
                        setBUFRvalue(ibufr, r2['standard_name'], float(r1[r2['CSV_column']]))                   
                    elif str(r2['type']) in 'str':                   
                        setBUFRvalue(ibufr, r2['standard_name'], str(r1[r2['CSV_column']])) 
                
                
            #Set monitoring time period (-10=10 minutes)
            if r1['WindSpeed(m/s)'] != -999:
                codes_set(ibufr, '#11#timePeriod', -10)
            if r1['ShortwaveRadiationDown_Cor(W/m2)'] != -999:
                codes_set(ibufr, '#14#timePeriod', -10)
            if r1['LongwaveRadiationDown(W/m2)'] != -999:
                codes_set(ibufr, '#15#timePeriod', -10)
            
            #Time significance 2 = time averaged
            codes_set(ibufr, '#1#timeSignificance', 2)
            if r1['WindSpeed(m/s)'] != -999:
                codes_set(ibufr, '#2#timeSignificance', 2)

            #Set measurement heights
            if r1['HeightSensorBoom(m)'] != -999:
                codes_set(ibufr, 
                          '#2#heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform', 
                          r1['HeightSensorBoom(m)']-0.1)
                codes_set(ibufr, 
                          '#8#heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform', 
                          r1['HeightSensorBoom(m)']+0.4)
                if r1['ElevationGPS(m)'] != -999:
                    codes_set(ibufr, 'heightOfBarometerAboveMeanSeaLevel', 
                              r1['ElevationGPS(m)']+r1['HeightSensorBoom(m)'])
 
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
    txtFiles = glob.glob('AWS_data/*hour*')
    
    #Get lookup table
    lookup = getTXT('./variables_bufr.csv', None)
 
    #Get all txt files in directory
    outFiles = './BUFR_out/'
    if os.path.exists(outFiles) is False:
        os.mkdir(outFiles)
       
    # #Iterate through txt files
    for fname in txtFiles:
    
        #Generate output BUFR filename
        bufrname = fname.split('/')[-1].split('.txt')[0][:-4][:-5]+'.bufr'
        print(f'Generating {bufrname} from {fname}')
    
        #Get text file
        df1 = getTXT(fname)
        
        #Get Kelvin temperature        
        df1['AirTemperature(K)'] = df1.apply(lambda row: getTempK(row), axis=1)
        
        #Get Pa pressure
        df1['AirPressure(Pa)'] = df1.apply(lambda row: getPressPa(row), axis=1)         
        
        #Construct and export BUFR file
        getBUFR(df1, lookup, outFiles+bufrname)
        print(f'Successfully exported bufr file to {outFiles+bufrname}')   
        
        
    print('Finished')
