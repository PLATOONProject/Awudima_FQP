

from awudima.pyrml.model import *

if __name__ == "__main__":
    rml_source = RMLSource(name='#DB_Source', dstype=DataSourceType.MYSQL,
                        ds_desc={'jdbcDSN': 'jdbc:mysql://mysql:3306/benchmark',
                                 'jdbcDriver': 'com.mysql.cj.jdbc.Driver',
                                 'username': 'user',
                                 'password': 'user',
                                 'sqlVersion': 'SQL2008',
                                 'tableName': 'benchmark.offer'})

    ls = LogicalSource(source=rml_source, logical_iterator='row', reference_formulation='rr:SQL2008')
    subj = TermMap(
         value='http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer{producer}/Product{nr}',
         resource_type=TripleMapType.TEMPLATE, term_type=TermType.IRI)

    sm = SubjectMap(smap_id="Product", subject=subj,
                     rdf_types=['http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product'])

    p = TermMap(value="http://www.w3.org/2000/01/rdf-schema#comment",
                 resource_type=TripleMapType.CONSTANT,
                 term_type=TermType.IRI)
    pm = PredicateMap(predmap_id='http://www.w3.org/2000/01/rdf-schema#comment', predicate=p)
    o = TermMap(value='labelid',
                 resource_type=TripleMapType.REFERENCE,
                 term_type=TermType.Literal)
    om = ObjectMap(objmap_id='someid', theobject=o, datatype=XSD.string, language=None)
    pom = [(pm, om)]

    p = TermMap(value="http://www.w3.org/2000/01/rdf-schema#type",
                 resource_type=TripleMapType.CONSTANT,
                 term_type=TermType.IRI)
    pm = PredicateMap(predmap_id='rdf-schema#type', predicate=p)
     # parenttm = TripleMap(uri=, logical_source=, subject_map=, predicate_obj_map=None)

    o = ObjectReferenceMap(parentmap='ProductHasProductFeature', join_conditions=[('nr', 'product')])
    om = ObjectMap(objmap_id='someid', theobject=o, language=None)
    pom.append((pm, om))

    tmp = TripleMap("mytriplemap", logical_source=ls, subject_map=sm, predicate_obj_map=pom)
    from pprint import pprint

    pprint(tmp.to_json())
    pprint(tmp)
