from faker import Faker
import numpy as np
import dask.array as da
import dask
import sys
from mygenerator import *

def get_birthdate(n):
    # This function generates "date_of_birth" field because mimesis doesn't include this field
    return np.array([fake.date_of_birth() for i in range(n)])

if __name__ == "__main__":
    
    #initialising global variables
    npart=100
    n_records=50000
    outputfilename = 'people.csv'
    records_per_part = 0

    #Read command line arguments, set globale variables
    nargs = len(sys.argv)
    if nargs <3 :
        print("Format  : python makepeoplecsv.py numberofrecords outputfilename")
        print("Default : python makepeoplecsv.py 50000 people.csv")
    try:
        if nargs >=2 :
            n_records = int(sys.argv[1])
        if nargs >=3 :
            outputfilename = sys.argv[2]
    except:
        print('Bad arguments')
        sys.exit()
    if n_records%npart != 0:
        print("number of records must be divisible by number of partitions")
        sys.exit()
    records_per_part = n_records//npart
    
    #Initialize data producer object. It is used only to produce "date_of_birth" field values
    print("Initialising .... ")
    fake = Faker()
    
    #Make_people returns a dask bag including "first_name", "last_name" and "address" columns
    bag_people=make_people(npart, records_per_part)
    #convert dask bag to dask dataframe
    df_people = bag_people.to_dataframe()
    
    #Make "date_of_birth" column and create a dask array
    arr_birthdate = da.from_delayed(
                    dask.delayed(get_birthdate)(n_records), 
                    shape=(n_records,), dtype=str
                    )                                               \
                    .rechunk(records_per_part)
    
    # Add fourth column to the main dataframe
    df_people['date_of_birth']=arr_birthdate
    # Compute dask delayed calculations and write to csv file
    print("Running .... ")
    df_people.compute().to_csv(outputfilename, index=False)
    print(f"File {outputfilename}  has been created with {n_records} records in the current directory")