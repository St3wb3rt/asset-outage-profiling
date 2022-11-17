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

df_names = []
y = 0
for row in df['messageID'].unique():
    df = df.loc[df[row.messageID]]
    df_names.append(df['messageID'].unique())
    df_names[y] = pd.DataFrame(df, columns=['messageID', 'sequenceID', 'publishedDateTime', 'assetID', 'assetEICCode',
                                            'affectedUnit', 'assetFuelType', 'assetNormalCapacity', 'availableCapacity',
                                            'eventStart', 'eventEnd', 'cause', 'revisionNumber', 'unavailableCapacity',
                                            'relatedInformation'])
    y = y + 1

print(df_names[4])
x = 0
# for row in dff.iterrows():
#
#     date_diff = df_names[x].loc[x].at['eventEnd'] - df_names[x].loc[x].at['eventStart']
#     date_diff = datetime.timedelta.total_seconds(date_diff)
#     ten_min = date_diff / 600
#     ten_min = int(ten_min)
#
#     messageID = df_names[x].loc[x].at['messageID']
#     #df_filtered = dff.loc[(dff.messageID == messageID)]
#     print(messageID)
#
#     sequenceID = df_names[x].loc[x].at['sequenceID']
#     publishedDateTime = df_names[x].loc[x].at['publishedDateTime']
#     assetID = df_names[x].loc[x].at['assetID']
#     assetEICCode = df_names[x].loc[x].at['assetEICCode']
#     affectedUnit = df_names[x].loc[x].at['affectedUnit']
#     assetFuelType = df_names[x].loc[x].at['assetFuelType']
#     assetNormalCapacity = df_names[x].loc[x].at['assetNormalCapacity']
#     availableCapacity = df_names[x].loc[x].at['availableCapacity']
#     eventEnd = df_names[x].loc[x].at['eventEnd']
#     cause = df_names[x].loc[x].at['cause']
#     revisionNumber = df_names[x].loc[x].at['revisionNumber']
#     unavailableCapacity = df_names[x].loc[x].at['unavailableCapacity']
#     relatedInformation = df_names[x].loc[x].at['relatedInformation']
#
# #     for i in range(ten_min):
# #
# #         eventStart = (df_filtered['eventStart'].iloc[-1] + datetime.timedelta(minutes=10))
# #         new_row = pd.Series({'messageID': messageID, 'sequenceID': sequenceID, 'publishedDateTime': publishedDateTime,
# #                              'assetID': assetID, 'assetEICCode': assetEICCode, 'affectedUnit': affectedUnit,
# #                              'assetFuelType': assetFuelType, 'assetNormalCapacity': assetNormalCapacity,
# #                              'availableCapacity': availableCapacity, 'eventStart': eventStart, 'eventEnd': eventEnd,
# #                              'cause': cause, 'revisionNumber': revisionNumber,
# #                              'unavailableCapacity': unavailableCapacity, 'relatedInformation': relatedInformation})
# #
# #         df_final = pd.concat([df_final, new_row.to_frame().T], ignore_index=True)
# #
# #     new_row = pd.Series({'messageID': messageID, 'sequenceID': sequenceID, 'publishedDateTime': publishedDateTime,
# #                          'assetID': assetID, 'assetEICCode': assetEICCode, 'affectedUnit': affectedUnit,
# #                          'assetFuelType': assetFuelType, 'assetNormalCapacity': assetNormalCapacity,
# #                          'availableCapacity': availableCapacity, 'eventStart': eventEnd, 'eventEnd': eventEnd,
# #                          'cause': cause, 'revisionNumber': revisionNumber, 'unavailableCapacity': unavailableCapacity,
# #                          'relatedInformation': relatedInformation})
# #
# #     df_final = pd.concat([df_final, new_row.to_frame().T], ignore_index=True)
# #
# #     df_final['sequenceID'] = df_final['sequenceID'].astype(int)
# #     #df_final['publishedDateTime'] = df_final['publishedDateTime'].astype('datetime64[ns]')
# #     df_final['assetNormalCapacity'] = df_final['assetNormalCapacity'].astype(int)
# #     df_final['availableCapacity'] = df_final['availableCapacity'].astype(int)
# #     df_final['revisionNumber'] = df_final['revisionNumber'].astype(int)
# #     df_final['unavailableCapacity'] = df_final['unavailableCapacity'].astype(int)
# #     x = x + 1
# #
# # print(df_final.dtypes)
# # df_final = pd.DataFrame(df_final)
# #
# # df_final.to_csv('df_final.csv')
# #
# #
# #
# #
