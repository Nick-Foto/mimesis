import dask.dataframe as dd
import sys
from mygenerator import *


if __name__ == '__main__':

    #initialising global variables
    npart=100
    inputfilename = 'people.csv'
    
    #Read command line arguments, set globale variables
    nargs = len(sys.argv)
    if nargs <2 :
        print("Format  : python anonpeoplecsv.py inputfilename")
        print("Default : python anonpeoplecsv.py people.csv")
    if nargs >=2 :
        inputfilename = sys.argv[1]

    print("Initialising .... ")

    #Read main csv file and store its data in a dask dataframe
    df_people= dd.read_csv(inputfilename, usecols=['first_name','last_name','address','date_of_birth'])
    df_people.repartition(npart)

    #Extract fourth field "date_of_birth" from dataframe and store it in a dask array
    arr_birthdate = df_people.to_dask_array(lengths=True)[:,3]

    #Calculate number of records and number of records per partition
    n_records=arr_birthdate.shape[0]
    records_per_part= n_records // npart

    #Make_people returns a dask bag including "first_name", "last_name" and "address" columns
    bag_fake=make_people(npart, records_per_part)
    #convert dask bag to dask dataframe
    df_fake = bag_fake.to_dataframe()

    #Set number of chunks of fourth column and add it to main dataframe
    arr_birthdate = arr_birthdate.rechunk(records_per_part)
    df_fake['date_of_birth']=arr_birthdate

    #create outputfilename from inputfilename
    outputfilename = inputfilename.split('.')[0]+'faked.csv'
    print("Running .... ")

    # Compute dask delayed calculations and write to csv file
    df_fake.compute().to_csv(outputfilename, index=False)
    print(f"File with anonymised fields {outputfilename}  has been created with {n_records} records in the current directory")
