from sys import argv
from itertools import izip
import time, sqlite3

script, filename, indexfilename, echo = argv

# filename = "C:\Users\LocalAdminLeo\src\Kaggle Spring Leaf 1\\train.csv"
separator = ',' #CSV values are separated by commas
bounder = '''"''' # but in our CSV some entries also have double apostrophies around them
querylimit = 970 #there seems to be a limit on number of parameters one can specify with for a single query using {sqllite3} library. We can either modify this limit or break our query in two separate queries. We do the latter.


# Example:
# pairwise([0,1,2,3,4,5])
# >> [[0,1],[2,3],[4,5]]
def pairwise(iterable):
	a = iter(iterable)
	return izip(a, a)

# Example:
# chain([0,1,2,3])
# >> [[0,1],[1,2],[2,3]]
def chain(iterable):
	a,b = tee(iterable)
	next(b, None)
	return izip(a, b)

# Example:
# index_separator("Googool","o")
# >> [1,2,4,5]
def index_separator(line, separator):
    index = -1
    while True:
        index = line.find(separator, index + 1)
        if index == -1:
            break
        else:
            yield index

# We use this only for headers. Extracts a list of values between every consequtive pair of {bounder} there will be a cell value.
extract_values = lambda csvstr : map(lambda ind: csvstr[ ind[0] + 1 : ind[1] ], pairwise(index_separator(csvstr, bounder))) 

#####################
# Here's how to read the above from the innermost clause
# 1. {index_separator} will get all indices of the {bounder} in the {csvstr}
# 2. {pairwise} will arrange all indices returned by {index_separator} in pairs so that [r1,r2,...,rn-1, rn] will become [[r1,r2],...,[rn-1,rn]]
# 3.1 map() will apply an anonymous function to every element of the list returned by {pairwise}, i.e. to every pair of indices [r1,r2] etc
# 3.2 This anonymous function return all elements in {csvstr} strictly between the given indices. Indices are given from {pairwise}.
# 3.3 Note that when calling map() we use {csvstr}, but it is only provided by the higher level call.
# 3.4 I don't really know functional programming enough, so this might actually be bad. But it's techinically possible! We run this script just once after all.
# 4. {extract_values} will just supply this {csvstr} variable to the map and whatever is inside
# Note that the below line is much more readable than the above approach. But I found the above tricks while browsing for fast file reading and its good to learn.
# headers = map( lambda word: word[1:-1], first_line.split(separator)) # split the file by {separator} and get rid of first and last symbol
#####################

# Form a string of {n} '(?)' to use the dedicated DBAPI syntax for SQL queries instead of usual Python
dbapiparams = lambda n : '''(?''' + ''',?'''*(n-1) + ''')''' 

#####################
# Note, that all SQL queries should be formed with (?,..) syntax(DBAPI syntax), rather than with Python %s syntax for strings.
# But as an exception, we can't use (?,..) for table name and headers.
# The above two lines do the same as below. 
# def py_str_arg_list(n):
# 	paramstr = []
# 	for i in range(n):
# 		paramstr += '{'+str(i)+'}, '
# 	return paramstr
#####################


# Take a number j and return a string '{j}'
py_str_arg = lambda j: '{' + str(j) + '}' 
# Take a number n and return a string '{0}, {1}, ..., {n-1}'
py_str_arg_list = lambda n :', '.join(map(py_str_arg, range(n))) 

#Take a list of strings, and return a single string formatted as a parametrized SQL UPDATE query, with column names as elements of that given list.
# Example:
# sqlupdateparams(['one','two','three'])
# >> 'one=?, two=?, three=?'
sqlupdateparams = lambda columns: ', '.join(map(lambda x: x+'=?', columns))



start_time = time.time()

# Create a database by filename or connect to an existing one
conn = sqlite3.connect(indexfilename) 
# Create a cursor to browse a database
cur = conn.cursor() 



with open(filename,'rt') as f:
	headers = extract_values(f.readline()) #get first file line and extract header names from it

	header_query = ('''CREATE TABLE rawdata (''' + py_str_arg_list(len(headers)) + ''')''').format(*headers) #form an SQL query that creates a table with headers extracted from CSV

	cur.execute(header_query) #execute the SQL query that we formed - {header_query} by calling the execute() method

	for lineind, line in enumerate(f): #enumerate() doesn't create a new list, but only a generator so we can get line numbers
		if lineind > 0: #all table INSERTs will start from second file line
			linevalues = line.split(separator)
			if len(linevalues) == len(headers): #some CSV lines are seemingly longer than the header line, we drop them
				# datalist = clean_quotes(linevalue) #{clean_quotes} doesn't exist atm, but could in principle do additional data cleaning
				datalist = linevalues
				if not line == '': #empty lines are dropped
					# insert_query = '''INSERT INTO rawdata ''' + ''' VALUES ''' + dbapiparams(len(datalist)) #we can't do one single query right now because with more than 1400 parameters we exceed the variable limit set by sqlite3
					insert_query = '''INSERT INTO rawdata (''' + ''','''.join(headers[:querylimit]) + ''') VALUES ''' + dbapiparams(querylimit) #form an SQL query to create 1 table line using DBAPI syntax
					update_query = '''UPDATE rawdata SET ''' + sqlupdateparams(headers[querylimit:]) + ''' WHERE ''' + headers[0] + '''=?'''# do same as above for remaining values

					cur.execute(insert_query, tuple(datalist[:querylimit])) # execute the INSERT query by giving list of headers and list of values as query parameters
					cur.execute(update_query, tuple(datalist[querylimit:] + [datalist[0]])) # last supplied parameter is the ID which is the first in {datalist}. We supply a tuple after concatenating two lists of strings appropriately.
		
		conn.commit() #commit the changes to the database

		if echo == 'echo': # output progress every fixed number of lines; but checking this condition and printing slows it down a little bit
			if lineind%(3*7*11) == 1:
				print "Got through %d lines.." % lineind

conn.close() #close the connection to the DB just like we have to close files


print "Indexed all lines in %d seconds" % (time.time() - start_time)
