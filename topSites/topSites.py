import psycopg2
from urllib.parse import urlparse as urlParse
from collections import Counter
import json


conn = None
def open_conn():
  try:
    conn = psycopg2.connect(
      host="localhost",
      port="40000",
      user="admin",
      password="12345",
      dbname="train_of_thought"
    )
    print("Conexão aberta")
    return conn

  except (Exception, psycopg2.DatabaseError) as error:
    print(error)

def close_conn(conn):
  if conn is not None:
    conn.close()
    print("Conexão fechada")

def setTopSites():
  try:
    conn = open_conn()
    cur = conn.cursor()

    userIds = getDistinctUserIds(cur)
    for userId in userIds:
      capturedUrls = getUserCapturedUrls(cur, userId)

      domains = []
      for capturedUrl in capturedUrls:
        domains.append(urlParse(capturedUrl[0]).netloc)

      topSites = json.dumps(dict(Counter(domains).most_common(10)))

      hasTopSites = countUserTopSites(cur, userId)

      if hasTopSites[0] == 0:
        insertTopSites(cur, userId, topSites)
      else:
        updateTopSites(cur, userId, topSites)

      conn.commit()
      print(f"Dados commitados para userId {userId[0]}")

  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
  finally:
    close_conn(conn)

def getDistinctUserIds(cur):
  cur.execute('SELECT DISTINCT "userId" FROM "Captures"')
  return cur.fetchall()

def getUserCapturedUrls(cur, userId):
  cur.execute(f'SELECT "capturedUrl" FROM "Captures" WHERE "userId" = {userId[0]}')
  return cur.fetchall()

def countUserTopSites(cur, userId):
  cur.execute(f'SELECT COUNT("userId") FROM "TopSites" WHERE "userId" = {userId[0]}')
  return cur.fetchone()

def insertTopSites(cur, userId, topSites):
  cur.execute(f'INSERT INTO "TopSites" VALUES({userId[0]}, \'{topSites}\')')

def updateTopSites(cur, userId, topSites):
  cur.execute(f'UPDATE "TopSites" SET "topSites" = \'{topSites}\' WHERE "userId" = {userId[0]}')


if __name__ == '__main__':
  setTopSites()
