import psycopg2

def open(**kwargs):
  print('Connecting to the PostgreSQL database...')
  try:
    conn = psycopg2.connect(**kwargs)
    print('Database connection opened.')
    return conn

  except (Exception, psycopg2.DatabaseError) as error:
    print(error)

def close(conn):
  if conn is not None:
    conn.close()
    print('Database connection closed.')
