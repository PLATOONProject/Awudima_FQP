from awudima.pyrdfmt.federation import Federation
from awudima.pyrdfmt.rdfmt import RDFMT
from awudima.pyrdfmt.predicate import MTPredicate
from awudima.pyrdfmt.datasource import DataSource
from awudima.pyrdfmt.extractor import RDFMTExtractor
from awudima.pyrdfmt.mongodb_store import MongoLDFlattenRDFMT
from awudima.pyrdfmt.sparql_endpoint import SPARQLEndpointRDFMT


if __name__ == '__main__':

    fed = Federation.config('../../config-pilot3a.json')
    from pprint import pprint
    pprint(fed.to_json())
    fed.extract_molecules()
    fed.dump_to_json('../../federation_3a.json')
    pprint(fed.to_json())
