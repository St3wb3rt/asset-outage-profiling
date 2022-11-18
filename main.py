# main.py

from elexon import ElexonRawClient
import datetime
import pandas as pd


# Set Elexon api key
my_api_key = 't2ovb8odenxim4h'
api = ElexonRawClient(my_api_key)

# Get current date/time and format validFrom and validUntil dates
currentDate = datetime.datetime.now()

# Define publicationFrom date - number of days in the past
dateDeltaPast = currentDate + datetime.timedelta(days=-7)
publicationFrom = dateDeltaPast.strftime('%Y-%m-%d')

# Define publicationTo date - number of days in the future
dateDeltaFuture = currentDate + datetime.timedelta(days=7)
publicationTo = dateDeltaFuture.strftime('%Y-%m-%d')

# Populate asset location information from json source
df_locations = pd.read_json('.\\Locations.json', typ='frame', precise_float=True)
df_locations['lon'] = df_locations['lon'].astype(float)
df_locations['lat'] = df_locations['lat'].astype(float)
df_locations['assetID'] = df_locations['assetID'].astype(str)

# Read the data
remit = api.request('MessageListRetrieval', PublicationFrom=publicationFrom, PublicationTo=publicationTo)
df = pd.DataFrame(remit, columns=['messageID', 'sequenceID', 'messageHeading', 'eventType', 'publishedDateTime',
                                  'participant_MarketParticipantID', 'assetID', 'assetEICCode', 'affectedUnit',
                                  'assetFuelType', 'assetNormalCapacity', 'availableCapacity', 'eventStart', 'eventEnd',
                                  'durationUncertainty', 'cause', 'eventStatus', 'activeFlag', 'revisionNumber',
                                  'messageType', 'unavailabilityType', 'unavailableCapacity', 'assetType',
                                  'affectedArea', 'relatedInformation'])

# Convert data type of 'eventStart' and 'eventEnd' columns to 'datetime64[ns]'.
df['eventStart'] = df['eventStart'].astype('datetime64[ns]')
df['eventEnd'] = df['eventEnd'].astype('datetime64[ns]')

# Filter Data by activeFlag, eventStatus and assetID must be present in the locations table
df = df.loc[(df.activeFlag != False) & (df.eventStatus == "Active") & (df.assetID.isin(df_locations['assetID']))]

y = 0
for index, row in df.iterrows():
    dff = df.loc[(df.messageID == row['messageID'])]
    x = 0
    dff.reset_index(drop=True, inplace=True)
    #print(dff.shape)

    for j in dff.itertuples():
        date_diff = dff.loc[x].at['eventEnd'] - dff.loc[x].at['eventStart']
        date_diff = datetime.timedelta.total_seconds(date_diff)
        ten_min = date_diff / 600
        ten_min = int(ten_min)
        print(ten_min)

        messageID = dff.loc[x].at['messageID']
        print(messageID)
        sequenceID = dff.loc[x].at['sequenceID']
        publishedDateTime = dff.loc[x].at['publishedDateTime']
        assetID = dff.loc[x].at['assetID']
        assetEICCode = dff.loc[x].at['assetEICCode']
        affectedUnit = dff.loc[x].at['affectedUnit']
        assetFuelType = dff.loc[x].at['assetFuelType']
        assetNormalCapacity = dff.loc[x].at['assetNormalCapacity']
        availableCapacity = dff.loc[x].at['availableCapacity']
        eventEnd = dff.loc[x].at['eventEnd']
        cause = dff.loc[x].at['cause']
        revisionNumber = dff.loc[x].at['revisionNumber']
        unavailableCapacity = dff.loc[x].at['unavailableCapacity']
        relatedInformation = dff.loc[x].at['relatedInformation']

        for z in range(ten_min):
            eventStart = (dff['eventStart'].iloc[-1] + datetime.timedelta(minutes=10))
            new_row = pd.Series({'messageID': messageID, 'sequenceID': sequenceID, 'publishedDateTime': publishedDateTime,
                                 'assetID': assetID, 'assetEICCode': assetEICCode, 'affectedUnit': affectedUnit,
                                 'assetFuelType': assetFuelType, 'assetNormalCapacity': assetNormalCapacity,
                                 'availableCapacity': availableCapacity, 'eventStart': eventStart, 'eventEnd': eventEnd,
                                 'cause': cause, 'revisionNumber': revisionNumber,
                                 'unavailableCapacity': unavailableCapacity, 'relatedInformation': relatedInformation})

            df_final = pd.concat([dff, new_row.to_frame().T], ignore_index=True)

        new_row = pd.Series({'messageID': messageID, 'sequenceID': sequenceID, 'publishedDateTime': publishedDateTime,
                             'assetID': assetID, 'assetEICCode': assetEICCode, 'affectedUnit': affectedUnit,
                             'assetFuelType': assetFuelType, 'assetNormalCapacity': assetNormalCapacity,
                             'availableCapacity': availableCapacity, 'eventStart': eventEnd, 'eventEnd': eventEnd,
                             'cause': cause, 'revisionNumber': revisionNumber, 'unavailableCapacity': unavailableCapacity,
                             'relatedInformation': relatedInformation})

        df_final = pd.concat([dff, new_row.to_frame().T], ignore_index=True)
        df_final.to_csv(messageID + '.csv')

        df_final['sequenceID'] = df_final['sequenceID'].astype(int)
        #df_final['publishedDateTime'] = df_final['publishedDateTime'].astype('datetime64[ns]')
        df_final['assetNormalCapacity'] = df_final['assetNormalCapacity'].astype(int)
        df_final['availableCapacity'] = df_final['availableCapacity'].astype(int)
        df_final['revisionNumber'] = df_final['revisionNumber'].astype(int)
        df_final['unavailableCapacity'] = df_final['unavailableCapacity'].astype(int)
    x = x + 1

#print(df_final.dtypes)
#df_final = pd.DataFrame(df_final)

df_final.to_csv('df_final.csv')

y = y + 1