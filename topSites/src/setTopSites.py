from urllib.parse import urlparse as urlParse
from collections import Counter
from json import dumps as jsonDumps

from . import databaseOperations as dbOperations

def setTopSites(cur, conn):
    userIds = dbOperations.getDistinctUserIds(cur)
    for userId in userIds:
      capturedUrls = dbOperations.getUserCapturedUrls(cur, userId)

      domains = []
      for capturedUrl in capturedUrls:
        domains.append(urlParse(capturedUrl[0]).netloc)

      topSites = jsonDumps(dict(Counter(domains).most_common(10)))

      hasTopSites = dbOperations.countUserTopSites(cur, userId)

      if hasTopSites[0] == 0:
        dbOperations.insertTopSites(cur, userId, topSites)
      else:
        dbOperations.updateTopSites(cur, userId, topSites)

      conn.commit()
      print(f"Dados commitados para userId {userId[0]}")
