

makepeoplecsv.py
	Creates csv file with four fields with random generated values 
		(first_name, last_name, address and date_of_birth)
	To run: python3 makepeoplecsv numberofrecords outputfilename
	Default value for numberofrecords is 50000
	Default value for outputfilename is people.csv
anonpeopplecsv.py
	1 - Reads csv file with four columns 
			(first_name, last_name, address and date_of_birth)
	2 - Anonymise three columns (first_name, lastname and address)
	3 - Writes result dataframe to a file renamed as filenamefaked.csv
	To run: python3 anonpeoplecsv.py inputfilename
	Default name for inputfilename is people.csv