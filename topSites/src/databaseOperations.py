def getDistinctUserIds(cur):
  cur.execute('SELECT DISTINCT "userId" FROM "Captures"')
  return cur.fetchall()

def getUserCapturedUrls(cur, userId):
  cur.execute(f'SELECT "capturedUrl" FROM "Captures" WHERE "userId" = {userId[0]}')
  return cur.fetchall()

def insertTopSites(cur, userId, topSites):
  cur.execute(f'INSERT INTO "TopSites" VALUES({userId[0]}, \'{topSites}\')')

def updateTopSites(cur, userId, topSites):
  cur.execute(f'UPDATE "TopSites" SET "topSites" = \'{topSites}\' WHERE "userId" = {userId[0]}')

def countUserTopSites(cur, userId):
  cur.execute(f'SELECT COUNT("userId") FROM "TopSites" WHERE "userId" = {userId[0]}')
  return cur.fetchone()
