import json
import ast
import psycopg2, psycopg2.extras
from string import maketrans

# Inputs to specify which database we want to communicate with
DB_DSN = "host='localhost' dbname='postgres' user='xxxxxxxxx' password='xxxxxxx'"

# Input data path
input_data = '../data/sample.txt'

def create_table(query):
  """
  creates a table onto database described on DB_DSN using the input query
  returns: nothing
  """
  try:
     conn = psycopg2.connect(dsn=DB_DSN)
     cur = conn.cursor()
     cur.execute(query)
     conn.commit()
  except psycopg2.Error as e:
     print e.message
  else:
     cur.close()
     conn.close()

def insert_sales_data(data):
  """
  inserts data into a existing table called totvs.sales in database described on DB_DSN 
  returns: nothing
  """
  try:
     sql = "INSERT INTO totvs.sales VALUES(%s, %s, %s, %s, %s, %s, %s)"
     conn = psycopg2.connect(dsn=DB_DSN)
     cur = conn.cursor()
     cur.executemany(sql, data)
     conn.commit()
  except psycopg2.Error as e:
     print e.message
  else:
     cur.close()
     conn.close()

def drop_table(my_table):
  """
  drops the table "my_table" from the database described on DB_DSN 
  returns: nothing
  """
  try:
     sql = "DROP TABLE IF EXISTS " + my_table + ";"
     conn = psycopg2.connect(dsn=DB_DSN)
     cur = conn.cursor()
     cur.execute(sql)
     conn.commit()
  except psycopg2.Error as e:
     print e.message
  else:
     cur.close()
     conn.close()

def parse_data(filename):
  """
  transforms a file with json into tuples
  returns: list of tuples
  """
  data = list()
  try:
    f = json.load(open(filename, 'r'))
    tx_id = 0
    for json_obj in f:
      try:
        sale_date = json_obj["ide"]["dhEmi"]["$date"]
        sale_date = sale_date[:19]
        sale_day, sale_time = sale_date.split("T")
        tx_type = json_obj["ide"]["natOp"]
        details = json_obj["dets"]
        tx_id += 1
        for detail in details:
          product = detail["prod"]["xProd"]
          units = detail["prod"]["qCom"]
          unit_price = detail["prod"]["vUnCom"]
          my_tuple = (tx_id, product, sale_day, sale_time, tx_type, units, unit_price)
          data.append(my_tuple)
      except:
        pass
  except Exception as e:
    print e

  return data


if __name__ == '__main__':

  print "******* dropping review table **********"
  drop_table('totvs.sales')

  print "******* creating table sales **********"
  sql = "create table totvs.sales (tx_id INT, product TEXT, date_str TEXT, time TEXT, tx_type TEXT, units FLOAT, unit_price FLOAT);"
  create_table(sql)

  print "******* parsing data **********"
  sales_data = parse_data(input_data)

  print "******* inserting data into sales table **********"
  insert_sales_data(sales_data)




