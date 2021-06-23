import dask.dataframe as dd
from dask.base import tokenize
from mimesis.schema import Field, Schema
import random
import dask.bag as db
import sys

def make_people(npartitions, records_per_partition, seed=None, locale="en"):
    schema = lambda field: {
        "first_name": field("person.first_name"),
        "last_name": field("person.last_name"),
        "address": field("address.address")+" "+field("address.city")
        }
    return _make_mimesis( {"locale": locale}, schema, npartitions, records_per_partition, seed  )

def _generate_mimesis(field, schema_description, records_per_partition, seed):
    field = Field(seed=seed, **field)
    schema = Schema(schema=lambda: schema_description(field))
    for i in range(records_per_partition):
        yield schema.create(iterations=1)[0]

def _make_mimesis(field, schema, npartitions, records_per_partition, seed=None):
    field = field or {}
    random_state = random.Random(seed)
    seeds = [random_state.randint(0, 1 << 32) for _ in range(npartitions)]
    name = "mimesis-" + tokenize( field, schema, npartitions, records_per_partition, seed    )
    dsk = { (name, i): (_generate_mimesis, field, schema, records_per_partition, seed) for i, seed in enumerate(seeds)   }
    return db.Bag(dsk, name, npartitions)


if __name__ == '__main__':

    npart=100
    inputfilename = 'people.csv'
    nargs = len(sys.argv)
    if nargs <2 :
        print("Format  : python anonpeoplecsv.py inputfilename")
        print("Default : python anonpeoplecsv.py people.csv")
    if nargs >=2 :
        inputfilename = sys.argv[1]

    print("Initialising .... ")

    df_people= dd.read_csv(inputfilename, usecols=['first_name','last_name','address','date_of_birth'])
    df_people.repartition(npart)

    arr_birthdate = df_people.to_dask_array(lengths=True)[:,3]

    n_records=arr_birthdate.shape[0]

    records_per_part= n_records // npart

    bag_fake=make_people(npart, records_per_part)
    df_fake = bag_fake.to_dataframe()

    arr_birthdate = arr_birthdate.rechunk(records_per_part)
    df_fake['date_of_birth']=arr_birthdate

    outputfilename = inputfilename.split('.')[0]+'faked.csv'
    print("Running .... ")
    df_fake.compute().to_csv(outputfilename, index=False)
    print(f"File with anonymised fields {outputfilename}  has been created with {n_records} records in the current directory")
