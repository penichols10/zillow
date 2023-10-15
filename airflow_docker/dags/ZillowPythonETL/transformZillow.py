import pandas as pd
import numpy as np
from datetime import datetime

def getNumberFromAddressStreet(addressStreet):
    '''
    Extract street number from addressStreet column
    '''
    number =  addressStreet.split(' ')[0]
    # Some houses have no street number
    if number.isnumeric():
        return int(number)
    return np.nan

def getStreetFromAddressStreet(addressStreet):
    '''
    Extract street from addressStreet column
    '''
    addressStreet =  addressStreet.split(' ')
    # Some houses have no street number - return entire addressstreet
    if addressStreet[0].isnumeric():
        return ' '.join(addressStreet[1:])
    return ' '.join(addressStreet)

def renameColumns(df):
    '''
    Makes columns more readable and drops some unneeded columns
    '''
    columns = ['hdpData.homeInfo.zpid','addressStreet', 'hdpData.homeInfo.unit', 'addressCity', 'addressState', 'addressZipcode','hdpData.homeInfo.homeType',  'beds', 'baths', 'area', 'pgapt',
            'hdpData.homeInfo.price', 'hdpData.homeInfo.priceForHDP', 'hdpData.homeInfo.taxAssessedValue', 'zestimate', 'hdpData.homeInfo.rentZestimate',
            'hdpData.homeInfo.daysOnZillow','hdpData.homeInfo.isPreforeclosureAuction',  'brokerName', 'latLong.latitude', 'latLong.longitude',  
            'hasOpenHouse', 'openHouseStartDate', 'openHouseEndDate', 'builderName',
            'hdpData.homeInfo.listing_sub_type.is_newHome']

    # Drop prefixes from columns
    df = df[columns]
    columns = [col.split('.')[-1] for col in columns]

    df.columns = columns
    df = df.rename({'pgapt':'homeStatus'}, axis=1)

    return df


def transformAddresses(df):
# Format addresses
    df['streetNumber'] = df['addressStreet'].map(getNumberFromAddressStreet)
    df['street'] = df['addressStreet'].map(getStreetFromAddressStreet)

    # Get streets with no unit name
    streets = []
    for idx, street in enumerate(df['street']):
            unit = df['unit'][idx]
            
            if type(unit) == str:
                    street = street[:len(street) - len(unit)]
            streets.append(street.strip())

    df['street'] = streets

    return df

def createUrls(df):
    '''
    Create URLs from zpids and addresses
    '''
    df['zpid'] = df['zpid'].astype(str)
    df['addressZipcode'] = df['addressZipcode'].astype(str)
    propertyURLprefix = 'https://www.zillow.com/homedetails/'
    propertyURLSuffix = df['addressStreet'].str.replace(' ', '-').str.replace('#', '') + '-' + df['addressCity'].str.replace(' ', '-') + '-' + df['addressState'] + '-' + df['addressZipcode'] + '/' + df['zpid'].astype(str) + '_zpid'
    propertyURL = propertyURLprefix + propertyURLSuffix
    df['url'] = propertyURL
    
    return df

def clean_data(ti):
    df = pd.read_csv('C:\\Users\\Patrick\\Documents\\python\\zillow\\data\\zillow_raw.csv')
    
    df = renameColumns(df)
    
    # Some cleaning - removes junk values
    df = df[~df['addressStreet'].str.lower().str.contains('homes available soon')]
    df = df[df['homeType'].str.lower() != 'lot']
    df = df.reset_index(drop=True)

    df = transformAddresses(df)

    # Datetime conversion
    df['openHouseStartDate'] = pd.to_datetime(df['openHouseStartDate'])
    df['openHouseEndDate'] = pd.to_datetime(df['openHouseEndDate'])
    
    # Miscellaneous value cleaning
    df['homeType'] = df['homeType'].map(lambda x: ' '.join(x.split('_')).title())
    
    # Handle nulls in columns to be dimensions
    df['unit'] = df['unit'].replace(np.nan, "No Unit")
    df['brokerName'] = df['brokerName'].replace(np.nan, "Broke Unknown")
    df['hasOpenHouse'] = df['hasOpenHouse'].replace(np.nan, False)
    df['openHouseStartDate'] = df['openHouseStartDate'].replace(pd.NaT, 'No Time')
    df['openHouseEndDate'] = df['openHouseEndDate'].replace(pd.NaT, 'No Time')
    df['builderName'] = df['builderName'].replace(np.nan, 'Builder Unknown')
    df['is_newHome'] = df['is_newHome'].replace(np.nan, False)
    df = df[~df['streetNumber'].isna()]
    
    # Save the data to disk
    ts = datetime.now().strftime('%m-%d-%Y_%H%M%S')
    filename = f'C:\\Users\\Patrick\\Documents\\python\\zillow\\data\\zillow_clean_{ts}.csv'
    
    df.to_csv(filename, index=False)
    ti.xcom_push(key='clean_filename', value=filename)
    return filename
    
