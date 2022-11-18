# main.py

import datetime
import asset_data as ad

# Get current date/time and format validFrom and validUntil dates
currentDate = datetime.datetime.now()

# Define publicationFrom date - number of days in the past
dateDeltaPast = currentDate + datetime.timedelta(days=-365)
publicationFrom = dateDeltaPast.strftime('%Y-%m-%d')

# Define publicationTo date - number of days in the future
dateDeltaFuture = currentDate
publicationTo = dateDeltaFuture.strftime('%Y-%m-%d')

# Declare assetID to pull data for
asset = 'T_KEAD-2'

# Call get_data function
ad.get_data(publicationFrom, publicationTo, asset)


