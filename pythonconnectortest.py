#written using Python 2.7.8
import mysql.connector
from yahoo_finance import Share



def lets_do_it(ticker, start_date, end_date):
	global cnx
	CONFIG_FILENAME = "config.txt"
	inFile = open(CONFIG_FILENAME, 'r')
	sqlLogin = inFile.read().split()
	sqlConfig = {
		'user': sqlLogin[1],
		'password': sqlLogin[3],
		'host': sqlLogin[5],
		'database': sqlLogin[7],
	}
	cnx = mysql.connector.connect(**sqlConfig)

	global cursor
	cursor = cnx.cursor()
	table_test(ticker + "_tbl")
	print "Table check done"
	print "Starting data retrieval"
	dicthist = pull_hist_data(ticker, start_date, end_date)
	print "Data retrieval done"
	add_data_sql(dicthist, ticker)
	print "All Systems Operational"

def pull_hist_data(ticker, start_date, end_date):
	return Share(ticker).get_historical(start_date, end_date) 

def add_data_sql(dicthist, ticker):
#Iterates over List of Dictionaries passed off from pull_hist_data fuction	
#Places data specified into SQL database table.
#In each iteration table is checked to see if date already exists and skips if it does.
	for d in dicthist:
		stmt = ("SELECT date_fld FROM " + ticker + "_tbl " + "WHERE date_fld = %s ")
		cursor.execute(stmt, (d['Date'],))
		result = cursor.fetchone()
		if result:
			print "Skipped"
			next
			
		else:
			add_day = ("INSERT INTO " + ticker + "_tbl" + """
				   (date_fld, volume_fld, adjclose_fld) 
				   VALUES (%s, %s, %s)""")

			data_day = (d['Date'], d["Volume"], d["Adj_Close"])
			cursor.execute(add_day, data_day)
			print "Added " + str(d['Date'])

	cnx.commit()
	cnx.close()

def table_test(table_name):
#Checks database for existing table and creates it if it does not.

	stmt = ("SHOW TABLES LIKE %s ")

	cursor.execute(stmt, (table_name,))
	result = cursor.fetchone()
	if result:
		print 'Table exists'
		return True
	else:
		print 'Table does not exist'
		create_table = ("CREATE TABLE " + table_name + """ 
				(date_fld date, 
				volume_fld int, 
				adjclose_fld float)""") 
		cursor.execute(create_table)
		print 'Table created'


lets_do_it(raw_input("Enter Ticker: "), raw_input("Enter Start Date: "), raw_input("Enter End Date : "))
