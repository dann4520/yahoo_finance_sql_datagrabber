#written using Python 2.7.8

import mysql.connector
from yahoo_finance import Share
from Tkinter import *
import ttk

# Main function performs calculations from entry data
def Main(*args):
    global cnx
    global cursor

    ticker = tickerEntry.get().lower()
    startDate = startDateEntry.get()
    endDate = endDateEntry.get()

    labelString = ""

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

    cursor = cnx.cursor()

    labelString += table_test(ticker + "_tbl", labelString)

    labelString += "Table check done\n"
    labelString += "Starting data retrieval\n"

    dicthist = pull_hist_data(ticker, startDate, endDate)

    labelString += "Data retrieval done\n"
    labelString += add_data_sql(dicthist, ticker, labelString)
    labelString += "All Systems Operational\n"

    # write debug data to debugOutput label
    debugOutput.set(labelString)


def pull_hist_data(ticker, start_date, end_date):
    return Share(ticker).get_historical(start_date, end_date)

def add_data_sql(dicthist, ticker, labelString):
    #Iterates over List of Dictionaries passed off from pull_hist_data fuction
    #Places data specified into SQL database table.
    #In each iteration table is checked to see if date already exists and skips if it does.
    ticker = ticker.lower()
    table_name = ticker + "_tbl"
    if table_name[0] == '^':
        table_name = 'index' + table_name[1: len(table_name)]

    #added if statement to check for '^' since yahoo using this symbol to
    #signify an index
    date_fld = table_name + "date"
    volume_fld = table_name + "volume"
    adjclose_fld = table_name + "adjclose"
    skipcount = 0
    incount = 0
    for d in dicthist:
        stmt = ("SELECT " + date_fld + " FROM " + table_name + " WHERE " + date_fld + " = %s ")
        cursor.execute(stmt, (d['Date'],))
        result = cursor.fetchone()
        if result:
            labelString += "Skipped\n"
            skipcount += 1

        else:
            add_day = ("INSERT INTO " + table_name + " (" +
                       date_fld + ", " + volume_fld + ", " + adjclose_fld + ") " +
                       "VALUES (%s, %s, %s)")

            data_day = (d['Date'], d["Volume"], d["Adj_Close"])
            cursor.execute(add_day, data_day)
            labelString += "Added " + str(d['Date'] + "\n")
            incount += 1

    cnx.commit()
    cnx.close()
    labelString += str(skipcount) + " Records skipped\n"
    labelString += str(incount) + " Records inserted\n"
    return labelString

def table_test(table_name, labelString):
    #Checks database for existing table and creates it if it does not.
    if table_name[0] == '^':
        table_name = 'index' + table_name[1: len(table_name)]
    date_fld = table_name + "date"
    volume_fld = table_name + "volume"
    adjclose_fld = table_name + "adjclose"
    stmt = ("SHOW TABLES LIKE %s ")

    cursor.execute(stmt, (table_name,))
    result = cursor.fetchone()
    if result:
        labelString += "Table exists\n"
        pass
    else:
        labelString += "Table does not exist\n"
        create_table = ("CREATE TABLE " + table_name + " (" +
                        date_fld + " date, " + volume_fld + " int, " + adjclose_fld + " float)")

        cursor.execute(create_table)
        labelString += "Table created\n"
    return labelString

root = Tk()
root.title("Dan Investment Helper")

mainframe = ttk.Frame(root, padding="200 200 200 200")
mainframe.grid(column=0, row=0, sticky=(N, W, E, S))
mainframe.columnconfigure(0, weight=1)
mainframe.rowconfigure(0, weight=1)

# Ticker
tickerEntry = StringVar()
ttk.Label(mainframe, text="Ticker").grid(column=1, row=1, sticky=(W, E))
ticker_entry = ttk.Entry(mainframe, width=7, textvariable=tickerEntry)
ticker_entry.grid(column=1, row=2, sticky=(W, E))

# Start Date
startDateEntry = StringVar()
ttk.Label(mainframe, text="Start Date").grid(column=2, row=1, sticky=(W, E))
ticker_entry = ttk.Entry(mainframe, width=7, textvariable=startDateEntry)
ticker_entry.grid(column=2, row=2, sticky=(W, E))

# End Date
endDateEntry = StringVar()
ttk.Label(mainframe, text="End Date").grid(column=3, row=1, sticky=(W, E))
ticker_entry = ttk.Entry(mainframe, width=7, textvariable=endDateEntry)
ticker_entry.grid(column=3, row=2, sticky=(W, E))

# Button that sends entry data to Main() function
ttk.Button(mainframe, text="Download Historical Data", command=Main).grid(column=1, row=3, sticky=W)

# Make debug window
debugOutput = StringVar()
ttk.Label(mainframe, textvariable=debugOutput,).grid(column=1, row=4, columnspan=3, sticky=(W, E))

# come back at some later time and make this a textbox
#debugText = Text(root, height=2, width=30)

# put fancy padding around all gui elements
for child in mainframe.winfo_children(): child.grid_configure(padx=5, pady=5)

# run the Tkinter mainloop
root.mainloop()
