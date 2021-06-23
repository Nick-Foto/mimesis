import random
from mimesis.schema import Field, Schema
from dask.base import tokenize
import dask.bag as db

def make_people(npartitions, records_per_partition, seed=None, locale="en"):
    # This function generates random fields for first_name, last_name and address
    # Returns a dask bag including 3 columns
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


