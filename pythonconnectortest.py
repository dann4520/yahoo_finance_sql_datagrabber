import mysql.connector
from yahoo_finance import Share



def lets_do_it(ticker, start_date, end_date):
	global cnx
	cnx = mysql.connector.connect(user='pythoning', password='dan520woo',
	                              host='192.168.0.22',
	                              database='financedata')
	global cursor
	cursor = cnx.cursor()
	table_test(ticker + "_tbl")
	dicthist = pull_hist_data(ticker, start_date, end_date)
	add_data_sql(dicthist, ticker)

def pull_hist_data(ticker, start_date, end_date):
	return Share(ticker).get_historical(start_date, end_date) 

def add_data_sql(dicthist, ticker):
#Iterates over List of Dictionaries passed off from pull_hist_data fuction	
#Places data specified into SQL database table.
	for d in dicthist:

		add_day = ("INSERT INTO " + ticker + "_tbl" + """
			   (date_fld, volume_fld, adjclose_fld) 
               		   VALUES (%s, %s, %s)""")

		data_day = (d['Date'], d["Volume"], d["Adj_Close"])
		cursor.execute(add_day, data_day)

	cnx.commit()
	cnx.close()

def table_test(table_name):

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

#table_test(raw_input("please enter table name: "))

lets_do_it(raw_input("Enter Ticker: "), raw_input("Enter Start Date: "), raw_input("Enter End Date :"))
