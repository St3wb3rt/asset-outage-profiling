# main.py

import datetime
import keadby as k1
import keadby2 as k2
import peterhead as p1
import medway as m1

# Get current date/time and format validFrom and validUntil dates
currentDate = datetime.datetime.now()

# Define publicationFrom date - number of days in the past
dateDeltaPast = currentDate + datetime.timedelta(days=-7)
publicationFrom = dateDeltaPast.strftime('%Y-%m-%d')

# Define publicationTo date - number of days in the future
dateDeltaFuture = currentDate
publicationTo = dateDeltaFuture.strftime('%Y-%m-%d')

k2.get_keadby2(publicationFrom, publicationTo)
k1.get_keadby1(publicationFrom, publicationTo)
p1.get_peterhead1(publicationFrom, publicationTo)
m1.get_medway1(publicationFrom, publicationTo)
