# peterhead.py

from elexon import ElexonRawClient
import datetime
import pandas as pd

# Set Elexon api key
my_api_key = 't2ovb8odenxim4h'
api = ElexonRawClient(my_api_key)

def get_peterhead1(publicationfrom, publicationto):
    # Read the data
    remit = api.request('MessageListRetrieval', PublicationFrom=publicationfrom, PublicationTo=publicationto)
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
    df = df.loc[(df.activeFlag != False) & (df.eventStatus == "Active") & (df.assetID == 'T_PEHE-1')]

    # Iterate through each unique messageID
    y = 0
    for index, row in df.iterrows():
        dff = df.loc[(df.messageID == row['messageID'])]
        dff.reset_index(drop=True, inplace=True)

        x = 0
        for j in dff.itertuples():
            date_diff = dff.loc[x].at['eventEnd'] - dff.loc[x].at['eventStart']
            date_diff = datetime.timedelta.total_seconds(date_diff)
            ten_min = date_diff / 600
            ten_min = int(ten_min)

            # Declare variables for use in writing new rows to dataframe
            messageID = dff.loc[x].at['messageID']
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

            # Create df_final dataframe with required columns
            df_final = pd.DataFrame(dff, columns=['messageID', 'sequenceID', 'publishedDateTime', 'assetID', 'assetEICCode',
                                                  'affectedUnit', 'assetFuelType', 'assetNormalCapacity',
                                                  'availableCapacity', 'eventStart', 'eventEnd', 'cause', 'revisionNumber',
                                                  'unavailableCapacity', 'relatedInformation'])
            # Loops through once for every 10 minute period in the timedelta between eventStart and eventEnd
            u = 0
            for z in range(ten_min):
                df_final.reset_index(drop=True, inplace=True)
                eventStart = (df_final.loc[u].at['eventStart'] + datetime.timedelta(minutes=10))
                new_row = pd.Series(
                    {'messageID': messageID, 'sequenceID': sequenceID, 'publishedDateTime': publishedDateTime,
                     'assetID': assetID, 'assetEICCode': assetEICCode, 'affectedUnit': affectedUnit,
                     'assetFuelType': assetFuelType, 'assetNormalCapacity': assetNormalCapacity,
                     'availableCapacity': availableCapacity, 'eventStart': eventStart, 'eventEnd': eventEnd,
                     'cause': cause, 'revisionNumber': revisionNumber,
                     'unavailableCapacity': unavailableCapacity, 'relatedInformation': relatedInformation})
                # Check to make sure if the new row eventStart would be equal to the eventEnd then it is not written
                if eventStart < eventEnd:
                    df_final = pd.concat([df_final, new_row.to_frame().T], ignore_index=True)
                else:
                    break
                u = u + 1

            # Write a new row where eventStart = eventEnd to ensure the complete range of the outage is captured
            new_row = pd.Series({'messageID': messageID, 'sequenceID': sequenceID, 'publishedDateTime': publishedDateTime,
                                 'assetID': assetID, 'assetEICCode': assetEICCode, 'affectedUnit': affectedUnit,
                                 'assetFuelType': assetFuelType, 'assetNormalCapacity': assetNormalCapacity,
                                 'availableCapacity': availableCapacity, 'eventStart': eventEnd, 'eventEnd': eventEnd,
                                 'cause': cause, 'revisionNumber': revisionNumber,
                                 'unavailableCapacity': unavailableCapacity,
                                 'relatedInformation': relatedInformation})

            df_final = pd.concat([df_final, new_row.to_frame().T], ignore_index=True)
            df_final['sequenceID'] = df_final['sequenceID'].astype(int)
            df_final['assetNormalCapacity'] = df_final['assetNormalCapacity'].astype(int)
            df_final['availableCapacity'] = df_final['availableCapacity'].astype(int)
            df_final['revisionNumber'] = df_final['revisionNumber'].astype(int)
            df_final['unavailableCapacity'] = df_final['unavailableCapacity'].astype(int)

            df_final.to_csv(messageID + '.csv')
        x = x + 1
    y = y + 1
