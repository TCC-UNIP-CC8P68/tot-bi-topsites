import psycopg2
from urllib.parse import urlparse as urlParse
from collections import Counter
import json

def connect():
  conn = None
  try:
    conn = psycopg2.connect(
      host="localhost",
      port="40000",
      user="admin",
      password="12345",
      dbname="train_of_thought"
    )
    cur = conn.cursor()

    cur.execute('SELECT DISTINCT "userId" FROM "Captures"')
    userIds = cur.fetchall()
    for userId in userIds:
      cur.execute(f'SELECT "capturedUrl" FROM "Captures" WHERE "userId" = {userId[0]}')
      capturedUrls = cur.fetchall()

      domains = []      
      for capturedUrl in capturedUrls:
        domains.append(urlParse(capturedUrl[0]).netloc)

      topSites = json.dumps(dict(Counter(domains).most_common(10)))


      cur.execute(f'SELECT COUNT("userId") FROM "TopSites" WHERE "userId" = {userId[0]}')
      hasTopSites = cur.fetchone()

      if hasTopSites[0] == 0:
        cur.execute(f'INSERT INTO "TopSites" VALUES({userId[0]}, \'{topSites}\')')
      else:
        cur.execute(f'UPDATE "TopSites" SET "topSites" = \'{topSites}\' WHERE "userId" = {userId[0]}')

    conn.commit()

  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
  finally:
    if conn is not None:
      conn.close()
      print("Conexão fechada")

if __name__ == '__main__':
  connect()