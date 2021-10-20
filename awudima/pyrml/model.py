# -*- coding: utf-8 -*-
from __future__ import division, print_function

__version__ = '0.1'
__author__ = 'Kemele M. Endris'

# Modified: 29/04/2021
# based on: https://rml.io/specs/rml/

from enum import Enum
from typing import List, Dict, Tuple, Any
import rdflib
from rdflib import Namespace
from rdflib.namespace import XSD, RDF, RDFS, OWL


rml = Namespace("http://semweb.mmlab.be/ns/rml#")
rr = Namespace("http://www.w3.org/ns/r2rml#")
ql = Namespace("http://semweb.mmlab.be/ns/ql#")
d2rq = Namespace("http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#")

rrchild = rr.child
rrparent = rr.parent
rrclass = rr["class"]
rrdatatype = rr.datatype
rrconstant = rr.constant
rrgraph = rr.graph
rrgraphMap = rr.graphMap
rrinverseExpression = rr.inverseExpression
rrjoinCondition = rr.joinCondition
rrlanguage = rr.language
rrobject = rr.object
rrobjectMap = rr.objectMap
rrparentTriplesMap = rr.parentTriplesMap
rrpredicate = rr.predicate
rrpredicateMap = rr.predicateMap
rrpredicateObjectMap = rr.predicateObjectMap
rrsubject = rr.subject
rrsubjectMap = rr.subjectMap
rrtemplate = rr.template
rrtermType = rr.termType

rmllogicalSource = rml.logicalSource
rmlreference = rml.reference
rmliterator = rml.iterator
rmlreferenceFormulation = rml.referenceFormulation
rmlversion = rml.version
rmlsource = rml.source
rmlquery = rml.query

rrdefaultGraph = rr.defaultGraph
rmlJSONPath = rml.JSONPath
rrSQL2008 = rr.SQL2008
rrIRI = rr.IRI
rrBlankNode = rr.BlankNode
rrLiteral = rr.Literal
rmlXPath = rml.XPath

rmlBaseSource = rml.BaseSource
rrGraphMap = rr.GraphMap
rrJoin = rr.Join
rmlLogicalSource = rml.LogicalSource
rrObjectMap = rr.ObjectMap
rrPredicateMap = rr.PredicateMap
rrPredicateObjectMap = rr.PredicateObjectMap
rrRefObjectMap = rr.RefObjectMap
rrSubjectMap = rr.SubjectMap
rrTermMap = rr.TermMap
rrTriplesMap = rr.TriplesMap

rrcolumn = rr.coumn
rrsqlVersion = rr.sqlVersion
rrtableName = rr.tableName


class RMLSource(object):
    """
    Specifies the properties of the data source (rml:source) in logical source

    """

    def __init__(self, name, dstype=None, dbmstype=None, ds_desc=None):
        self.name = name
        self.dstype = dstype
        self.ds_desc = ds_desc

        self.dbmstype = dbmstype

    def to_json(self):

        return {
            'name': self.name,
            "dstype": self.dstype.value if isinstance(self.dstype, DataSourceType) else self.dstype,
            "ds_desc": self.ds_desc,
            "dbmstype": self.dbmstype.value if isinstance(self.dbmstype, DataSourceType) else self.dbmstype
        }

    @staticmethod
    def load_from_json(rmlsources):
        name = rmlsources['name']
        dstype = DataSourceType[rmlsources['dstype'].upper()] if rmlsources['dstype'] is not None else None
        ds_desc = rmlsources['ds_desc']
        dbmstype = DataSourceType[rmlsources['dbmstype'].upper()] if rmlsources['dbmstype'] is not None else None
        return RMLSource(name, dstype, dbmstype, ds_desc)

    def __repr__(self):
        if self.dstype is None:
            return '<' + str(self.name) + '>  a  <http://awudima.com/DataSource#FlatFile> . '

        if isinstance(self.dstype, DataSourceType):
            dsvalue = self.dstype.value
        else:
            dsvalue = self.dstype

        return '<' + str(self.name) + '>  a  ' + str(dsvalue) + " ; \n" + \
               '\t' + ";\n\t".join([pred + "   \"" + obj + '"' for pred, obj in self.ds_desc.items()]) + '.'

    def __str__(self):
        return self.__repr__()


class LogicalSource(object):
    """
    A logical source is any source that is mapped to RDF triples. A logical source is a Base Source, rml:BaseSource.

    Base Sources (rml:iterator, rml:logicalSource, rml:referenceFormulation, rml:source)
    A base source (rml:baseSource) is a logical source, rml:logicalSource,
    pointing to a source that contains the data to be mapped.
    At least the source, rml:source, of the data source and its logical iterator (rml:iterator), should be defined.

    A base source (rml:baseSource) is represented by a resource that has:
    - exactly one source (rml:source) property
    - exactly one logical iterator over the data source's rml:iterator
    - and zero or one reference formulation property (rml:referenceFormulation).

    """

    def __init__(self,
                 source: RMLSource,
                 logical_iterator,
                 reference_formulation=None,
                 query=None,
                 table_name=None,
                 sql_version=None):
        self.source = source
        self.logical_iterator = logical_iterator
        self.reference_formulation = reference_formulation
        self.table_name = table_name
        self.sql_version = sql_version
        self.query = query

    def to_json(self):
        rm = str(self.reference_formulation) if self.reference_formulation is not None else None
        sqlver = None
        if self.sql_version is not None:
            sqlver = self.sql_version[0] if isinstance(self.sql_version, tuple) else str(self.sql_version)

        return {
            "source": self.source.to_json(),
            "logical_iterator": self.logical_iterator,
            "reference_formulation": rm,
            'table_name': str(self.table_name) if self.table_name is not None else None,
            'query': str(self.query) if self.query is not None else None,
            'sql_version': sqlver
        }

    @staticmethod
    def load_from_json(logicalsource):
        source = RMLSource.load_from_json(logicalsource['source'])
        logic_itr = logicalsource['logical_iterator']
        ref_formulation = logicalsource['reference_formulation']
        tablename = logicalsource['table_name']
        query = logicalsource['query']
        sqlver = logicalsource['sql_version']

        return LogicalSource(source, logic_itr, ref_formulation, query, tablename, sqlver)

    def __repr__(self):
        refform = ('rml:referenceFormulation ' + str(
            self.reference_formulation) + ';\n\t\t') if self.reference_formulation is not None else ""
        query = ('rml:query "' + str(self.query.replace('\n', '  ')) + '";\n\t\t') if self.query is not None else ""
        tablename = ('rr:tableName "' + str(self.table_name) + '";\n\t\t') if self.table_name is not None else ""
        if self.sql_version is not None:
            sqlversion = 'rr:sqlVersion <' \
                         + (str(self.sql_version) if isinstance(self.sql_version, tuple) else str(self.sql_version))\
                         + "> ;\n\t\t"
        else:
            sqlversion = ""

        return 'rml:logicalSource [ \n\t\trml:source   <' + str(self.source.name) + '>;\n\t\t' + \
               'rml:iterator "' + str(self.logical_iterator) + '";\n\t\t' + \
               refform + sqlversion + query + tablename + ']'

    def __str__(self):
        return self.__repr__()


class SubjectMap(object):
    """
    As defined in [R2RML]: A subject map is a term map.

    It specifies a rule for generating the subjects of the RDF triples generated by a triples map.

    Typing Resources (rr:class)
    A subject map may have one or more class IRIs. They are represented by the rr:class property.
    The values of the rr:class property must be IRIs.
    For each RDF term generated by the subject map, RDF triples with predicate rdf:type and
    the class [IRI] as object will be generated.

    Mappings where the class [IRI] is not constant, but needs to be computed based on the contents of
    the logical source, can be achieved by defining a predicate-object map with predicate rdf:type and
    a non-constant object map.
    """

    def __init__(self, smap_id, subject=None, rdf_types=None):
        self.ID = smap_id
        self.subject = subject
        self.rdf_types = rdf_types
        if self.rdf_types is None:
            self.rdf_types = []
        if self.subject.term_type is None:
            self.subject.term_type = TermType.IRI

    def to_json(self):
        st = self.subject.to_json() if self.subject is not None else None
        return {
            "ID": self.ID,
            "subject": st,
            "rdf_types": self.rdf_types
        }

    @staticmethod
    def load_from_json(subjmap):
        sid = subjmap['ID']
        subj = TermMap.load_from_json(subjmap['subject'])
        rdftypes = subjmap['rdf_types']
        return SubjectMap(sid, subj, rdftypes)

    def __repr__(self):
        if self.subject is None:
            return ""
        if len(self.rdf_types) > 0:
            rrclasses = "\n\t\trr:class  " + ", ".join(['<' + c + '> ' for c in self.rdf_types])
        else:
            rrclasses = ""
        return '; \n\trr:subjectMap [\n\t\t' + str(self.subject) \
               + rrclasses + '\n\t\t] '

    def __str__(self):
        return self.__repr__()


class PredicateMap(object):
    """
    A predicate-object map is a function that creates one or more predicate-object pairs for each
    row/record/element/object of a logical source. It is used in conjunction with a subject map to
    generate RDF triples in a triples map.

    A predicate-object map is represented by a resource that references the following other resources:

    One or more predicate maps. Each of them may be specified in one of two ways:
        using the rr:predicateMap property, whose value must be a predicate map, or
        using the constant shortcut property rr:predicate.

    One or more object maps or referencing object maps. Each of them may be specified in one of two ways:
        using the rr:objectMap property, whose value must be either an object map, or a referencing object map.
        using the constant shortcut property rr:object.

    Both predicate maps and object maps are term maps.
    """

    def __init__(self, predmap_id, predicate):
        self.ID = predmap_id
        self.predicate = predicate
        if self.predicate.term_type is None:
            self.predicate.term_type = TermType.IRI

    def to_json(self):
        pt = self.predicate.to_json() if self.predicate is not None else None
        return {
            "ID": self.ID,
            "predicate": pt
        }

    @staticmethod
    def load_from_json(predmap):
        pid = predmap['ID']
        predicate = TermMap.load_from_json(predmap['predicate'])
        return PredicateMap(pid, predicate)

    def __repr__(self):
        if self.predicate is None:
            return ''

        if self.predicate.resource_type == TripleMapType.CONSTANT:
            return 'rr:predicate  <' + str(self.predicate.value) + '> '

        return 'rr:predicateMap  ' + str(self.predicate)

    def __str__(self):
        return self.__repr__()


class ObjectMap(object):
    """
    Object maps are term maps.

    may be specified in one of two ways:
    - using the rr:objectMap property, whose value must be either an object map, or a referencing object map.
    - using the constant shortcut property rr:object.

    """

    def __init__(self, objmap_id, theobject, datatype=None, language=None):
        self.ID = objmap_id
        self.objectt = theobject
        self.data_type = str(datatype) if datatype is not None else None
        self.language = language
        if self.objectt.term_type is None:
            if self.objectt.resource_type == TripleMapType.REFERENCE or self.data_type is not None or \
                    self.language is not None:
                self.objectt.term_type = TermType.Literal
            else:
                self.objectt.term_type = TermType.IRI

    def to_json(self):
        ot = None
        if self.objectt is not None:
            if isinstance(self.objectt, TermMap):
                ot = self.objectt.to_json()
            elif isinstance(self.objectt, ObjectReferenceMap):
                ot = self.objectt.to_json()
        return {
            "ID": self.ID,
            "objectt": ot,
            "data_type": self.data_type,
            "language": self.language
        }

    @staticmethod
    def load_from_json(objmap):
        oid = objmap['ID']

        if 'parent_map' in objmap['objectt']:
            objectt = ObjectReferenceMap.load_from_json(objmap['objectt'])
            datatype = None
            lang = None
        else:
            objectt = TermMap.load_from_json(objmap['objectt'])
            datatype = objmap['data_type']
            lang = objmap['language']
        return ObjectMap(oid, objectt, datatype, lang)

    def __repr__(self):
        if self.objectt is None:
            return ''

        if isinstance(self.objectt, TermMap):
            if self.objectt.resource_type == TripleMapType.CONSTANT:
                if self.objectt.term_type is TermType.IRI:
                    return "rr:object  <" + str(self.objectt.value) + "> ;"
                return "rr:object  \"" + str(self.objectt.value) + '" ;'

            return "rr:objectMap  [\n\t\t\t" + str(self.objectt) \
                   + ('\n\t\t\trr:datatype  <' + self.data_type + ">" if self.data_type is not None else "") \
                   + ('\n\t\t\trr:language ' + self.language if self.language is not None else "") \
                   + '\n\t\t\t]'
        else:
            return "rr:objectMap  [ \n\t\t\ta rr:RefObjectMap;" + str(self.objectt) + '\n\t\t\t]'


class TermMap(object):
    """
    As defined in R2RML (text adjusted to refer to data in other structured formats):

    An RDF term is either an [IRI], or a blank node, or a literal.
    A term map is a function that generates an RDF term from a logical reference.
    The result of that function is known as the term map's generated RDF term.
    Term maps are used to generate the subjects, predicates and objects of the RDF
    triples that are generated by a triples map. Consequently, there are several kinds of term maps,
    depending on where in the mapping they occur: subject maps, predicate maps, object maps and graph maps.

    A term map must be exactly one of the following:

        a constant-valued term map,
        a reference-valued term map,
        a template-valued term map.

    The references of a term map are the set of logical references referenced in the term map and depend
    on the type of term map.
    """

    def __init__(self, value, resource_type, term_type=None):
        self.value = value
        self.resource_type = resource_type
        self.term_type = term_type
        self.columns = self.get_columns()

    def to_json(self):
        tt = self.term_type.value if self.term_type is not None else None
        rt = self.resource_type.value if self.resource_type is not None else None
        return {
            "value": self.value,
            "resource_type": rt,
            "term_type": tt,
            "columns": self.columns
        }

    @staticmethod
    def load_from_json(termmap):
        value = termmap['value']
        restype = TripleMapType[termmap['resource_type'].upper()] if termmap['resource_type'] is not None else None
        termtype = None
        if termmap['term_type'] == TermType.IRI.value:
            termtype = TermType.IRI
        elif termmap['term_type'] == TermType.Literal.value:
            termtype = TermType.Literal
        elif termmap['term_type'] == TermType.BNode.value:
            termtype = TermType.BNode
        columns = termmap['columns']
        tm = TermMap(value, restype, termtype)
        tm.columns = columns
        return tm

    def get_columns(self):
        if self.resource_type == TripleMapType.REFERENCE:
            return [self.value]
        elif self.resource_type == TripleMapType.CONSTANT:
            return []

        template = self.value
        if template is None:
            return []
        splits = [st.split('{') for st in template.split('}')]
        splits = [st for st in splits if len(st) > 1 or (len(st) == 1 and len(st[0]) > 0)]
        splits = [st[1] for st in splits if len(st) == 2]
        self.columns = splits

        return self.columns

    def __repr__(self):
        if self.resource_type == TripleMapType.TEMPLATE:
            val = 'rr:template  '
        elif self.resource_type == TripleMapType.REFERENCE:
            val = 'rml:reference  '
        else:  # self.resource_type == TripleMapType.CONSTANT:
            val = 'rr:constant'
        if self.term_type is not None:
            return val + ' "' + self.value + "\" ; rr:termType  <" + str(self.term_type.value) + '> ;'
        else:
            return val + ' "' + self.value + '"'

    def __str__(self):
        return self.__repr__()

    def split_template(self):
        """
        Split RML TermMap template into array of arrays
        Example:
            template = 'http://ontario.com/resource/{name}/{id}/prop
            returns:
                [['http://ontario.com/resource/', 'name'], ['/', 'id'], ['prop']]

        :return:
        """
        if self.resource_type == TripleMapType.TEMPLATE:
            splits = [st.split('{') for st in self.value.split('}')]
            splits = [st for st in splits if len(st) > 1 or (len(st) == 1 and len(st[0]) > 0)]
            return splits
        elif self.resource_type == TripleMapType.REFERENCE:
            return [[self.value]]
        else:
            return []

    def as_expr(self):
        splits = self.split_template()
        # TODO: based on the type of tranlator return expression
        return splits


class ObjectReferenceMap(object):
    """
        A predicate-object map is a function that creates one or more predicate-object pairs for each
        row/record/element/object of a logical source. It is used in conjunction with a subject map to
        generate RDF triples in a triples map.

        A predicate-object map is represented by a resource that references the following other resources:

        One or more predicate maps. Each of them may be specified in one of two ways:
            using the rr:predicateMap property, whose value must be a predicate map, or
            using the constant shortcut property rr:predicate.

        One or more object maps or referencing object maps. Each of them may be specified in one of two ways:
            using the rr:objectMap property, whose value must be either an object map, or a referencing object map.
            using the constant shortcut property rr:object.

        Both predicate maps and object maps are term maps.
        """

    def __init__(self, parentmap, join_conditions: List[Tuple[Any, Any]], termtype=None):
        self.parent_map = parentmap
        self.join_conditions = join_conditions
        if self.join_conditions is None:
            self.join_conditions = []
        self.child_columns = [child_column for child_column, parent_column in self.join_conditions if
                              self.join_conditions is not None]
        self.parent_columns = [parent_column for child_column, parent_column in self.join_conditions if
                               self.join_conditions is not None]
        self.columns = self.child_columns
        self.term_type = termtype
        if self.term_type is None:
            self.term_type = TermType.IRI

        self.value = self.parent_map
        self.resource_type = TripleMapType.TRIPLEMAP

    def to_json(self):
        return {
            "parent_map": self.parent_map,
            "join_conditions": [[child_column, parent_column]
                                for child_column, parent_column in self.join_conditions
                                if self.join_conditions is not None],
            "term_type": self.term_type.value,
            "value": self.value,
            'resource_type': self.resource_type.value
        }

    @staticmethod
    def load_from_json(objrefmap):

        join_conditions_map = objrefmap['join_conditions']
        join_conditions = []
        for jc in join_conditions_map:
            if isinstance(jc, list) and len(jc) == 2:
                join_conditions.append((jc[0], jc[1]))

        parentmap = objrefmap['parent_map']
        # termtype = TermType[objrefmap['term_type']] if objrefmap['term_type'] is not None else None
        termtype = None
        if objrefmap['term_type'] == TermType.IRI.value:
            termtype = TermType.IRI
        elif objrefmap['term_type'] == TermType.Literal.value:
            termtype = TermType.Literal
        elif objrefmap['term_type'] == TermType.BNode.value:
            termtype = TermType.BNode

        return ObjectReferenceMap(parentmap, join_conditions, termtype)

    def __repr__(self):
        if self.parent_map is None:
            return ""

        val = "\n\t\t\trr:parentTriplesMap <" + self.value + ">"
        if self.join_conditions is not None:
            for child_column, parent_column in self.join_conditions:
                val += ";\n\t\t\trr:joinCondition [ " + "\n\t\t\t\trr:parent \"" + parent_column + "\";" \
                       + "\n\t\t\t\trr:child \"" + child_column + '"\n\t\t\t\t]'
        return val


class TermType(Enum):
    IRI = "http://www.w3.org/ns/r2rml#IRI"
    BNode = "http://www.w3.org/ns/r2rml#BlankNode"
    Literal = "http://www.w3.org/ns/r2rml#Literal"


class TripleMapType(Enum):
    TEMPLATE = "Template"
    CONSTANT = "Constant"
    REFERENCE = "Reference"
    TRIPLEMAP = "TripleMap"


class TripleMap(object):
    """
     A Triples Map defines rules to generate zero or more RDF triples sharing the same subject.

     A Triples Map consists of a Logical Source, a Subject Map and zero or more Predicate-Object Maps.

    A triples map specifies the rules for translating

    - each row of a database,
    - each record of a CSV or a TSV data source,
    - each element of an XML data source,
    - each object of a JSON data source,
    - etc...

      to zero or more RDF triples.

    A triples map is represented by a resource that references the following other resources:
    - must have exactly one LogicalSource (rml:logicalSource) property
    - must have exactly one subject map that specifies how to generate a subject for
    each row/record/element/object of the logical source.
        May be specified as subject map (rr:subjectMap) or constant shortcut property (rr:subject)
    - may have zero or more predicate-object-map (rr:predicateObjectMap) properties

    The references of all term maps of a triples map (subject map, predicate maps, object maps, graph maps)
    must be references to rows/records/elements/objects that exist in the term map's logical source.

    """

    def __init__(self, uri,
                 logical_source: LogicalSource = None,
                 subject_map: SubjectMap = None,
                 predicate_obj_map: List[Tuple[PredicateMap, ObjectMap]] = None):
        """
        :param uri:
        :param logical_source:
        :param subject_map:
        :param predicate_obj_map:
        """
        self.uri = uri
        self.logical_source = logical_source
        self.subject_map = subject_map
        self.predicate_obj_map = predicate_obj_map
        if self.predicate_obj_map is None:
            self.predicate_obj_map = []
        self.filename = None

    def to_json(self):
        ls = self.logical_source.to_json() if self.logical_source is not None else None
        sm = self.subject_map.to_json() if self.subject_map is not None else None
        pom = [{"predicateMap": pm.to_json(), "objectMap": om.to_json()} for pm, om in
               self.predicate_obj_map] if self.predicate_obj_map is not None else []
        return {
            "filename": self.filename,
            "uri": self.uri,
            "logical_source": ls,
            "subject_map": sm,
            "predicate_obj_map": pom
        }

    @property
    def const_predicate_object_maps(self):
        pred_obj_maps = {}
        for pm, om in self.predicate_obj_map:
            pred_obj_maps.setdefault(pm.predicate.value, []).append(om)

        rdf_type_obj_map = [ObjectMap("_:rdf_type", TermMap(mt, TripleMapType.CONSTANT, TermType.IRI))
                            for mt in self.subject_map.rdf_types]
        pred_obj_maps['http://www.w3.org/1999/02/22-rdf-syntax-ns#type'] = rdf_type_obj_map

        return pred_obj_maps

    def __repr__(self):
        subjmap = str(self.subject_map) if self.subject_map is not None else ""
        val = '<' + str(self.uri) + '>  a rml:TriplesMap;\n\t' + \
              str(self.logical_source) + ' ' + \
              subjmap
        for pm, om in self.predicate_obj_map:
            val += ';\n\trr:predicateObjectMap [\n\t\t' + str(pm) + ';\n\t\t' + str(om) + '\n\t\t]'
        val += '.'

        return val

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def load_from_json(mappings):
        rml_mappings = {}
        for tmid, mapping in mappings.items():
            filename = mapping['filename']
            uri = mapping['uri']
            logical_source = LogicalSource.load_from_json(mapping['logical_source'])
            subject_map = SubjectMap.load_from_json(mapping['subject_map'])
            predicate_obj_map = mapping['predicate_obj_map']
            predobjmap = []
            for pom in predicate_obj_map:
                predicatemap = PredicateMap.load_from_json(pom['predicateMap'])
                objectmap = ObjectMap.load_from_json(pom['objectMap'])
                predobjmap.append((predicatemap, objectmap))
            tm = TripleMap(uri, logical_source, subject_map, predobjmap)
            tm.filename = filename

            rml_mappings[uri] = tm
        return rml_mappings


prefixes = """
        prefix rr: <http://www.w3.org/ns/r2rml#> 
        prefix rml: <http://semweb.mmlab.be/ns/rml#> 
        prefix ql: <http://semweb.mmlab.be/ns/ql#> 
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
        prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
        prefix rev: <http://purl.org/stuff/rev#> 
        prefix schema: <http://schema.org/> 
        prefix xsd: <http://www.w3.org/2001/XMLSchema#> 
        prefix base: <http://tib.de/ontario/mapping#> 
        prefix iasis: <http://project-iasis.eu/vocab/> 
        prefix hydra: <http://www.w3.org/ns/hydra/core#> 
        """


class RML(object):

    def __init__(self, name, path, path_type, sources=None, triple_maps=None):
        self.name = name
        self.path = path
        self.path_type = path_type
        self.sources = sources
        if self.sources is None:
            self.sources = {}
        self.triple_maps = triple_maps
        if self.triple_maps is None:
            self.triple_maps = {}

    def to_json(self):
        return {
            "name": self.name,
            "path": self.path,
            "path_type": self.path_type,
            "sources": {sid: s.to_json() for sid, s in self.sources.items()},
            'triple_maps': {tn: tm.to_json() for tn, tm in self.triple_maps.items()}
        }

    @classmethod
    def load_from_json(cls, maps):
        name = maps['name']
        path = maps['path']
        path_type = maps['path_type']
        sources = {sid: RMLSource.load_from_json(s) for sid, s in maps['sources'].items()}
        triple_maps = {tn: TripleMap.load_from_json(tm) for tn, tm in maps['triple_maps'].items()}

        return RML(name, path, path_type, sources, triple_maps)

    def __repr__(self):
        if len(self.sources) > 0:
            sources = "\n ".join([str(s) for i, s in self.sources.items()])
        else:
            sources = ""

        triplemaps = "\n\n".join([str(tm) for i, tm in self.triple_maps.items()])

        return sources + "\n" + triplemaps

    def __str__(self):
        return self.__repr__()

    def load(self):
        if self.path_type == 'RML file':
            # graph = rdflib.Graph()

            if isinstance(self.path, list):

                for path in self.path:
                    ds = rdflib.Dataset()
                    g = ds.graph(path)
                    g.load(path, format=rdflib.util.guess_format(path))
                    triplemaps, sources = RML.get_triple_maps(ds)
                    self.triple_maps.update(triplemaps)
                    self.sources.update(sources)
            else:
                ds = rdflib.Dataset()
                g = ds.graph(self.path)
                g.load(self.path, format=rdflib.util.guess_format(self.path))
                triplemaps, sources = RML.get_triple_maps(ds)
                self.triple_maps.update(triplemaps)
                self.sources.update(sources)
        elif self.path_type == 'JSON':
            import json
            with open(self.path, 'r') as f:
                maps = json.load(f)
                mapping = RML.load_from_json(maps)
                self.triple_maps = mapping.triple_maps
                self.sources = mapping.sources
        else:
            # send sparql queries to the endpoint
            pass

    @staticmethod
    def get_triple_maps(ds):
        triplemaps = {}
        sources = {}
        for s, p, o, g in ds.quads((None, rmllogicalSource, None, None)):
            # get logical source
            ls = RML.get_logical_source(o, ds, g)
            # get subject map
            subjmap = RML.get_subject_map(s, ds, g)
            # get PredicateObjectMap
            pom = RML.get_predicate_obj_map(s, ds, g)

            tm = TripleMap(g.n3()[1:-1] + '_' + s.n3()[1:-1], logical_source=ls, subject_map=subjmap, predicate_obj_map=pom)
            tm.filename = g.n3()[1:-1]
            triplemaps[g.n3()[1:-1] + '_' + s.n3()[1:-1]] = tm
            sources[ls.source.name] = ls.source
        return triplemaps, sources

    @staticmethod
    def get_object_maps(o, ds, g):
        oms = []

        if (o, rrobject, None, g) in ds:
            for s2, p2, o2, g2 in ds.quads((o, rrobject, None, g)):

                if isinstance(o2, rdflib.URIRef):
                    obj = TermMap(value=o2.n3()[1:-1], resource_type=TripleMapType.CONSTANT, term_type=TermType.IRI)
                    om = ObjectMap(objmap_id=o2.n3(), theobject=obj)
                elif isinstance(o2, rdflib.Literal):
                    obj = TermMap(value=o2.n3()[1:-1], resource_type=TripleMapType.CONSTANT, term_type=TermType.Literal)
                    om = ObjectMap(objmap_id=o2.n3(), theobject=obj, datatype=o2.datatype, language=o2.language)
                else:
                    obj = TermMap(value=o2.n3(), resource_type=TripleMapType.CONSTANT, term_type=TermType.BNode)
                    om = ObjectMap(objmap_id=o2.n3(), theobject=obj)

                oms.append(om)
        else:
            for s2, p2, o2, g2 in ds.quads((o, rrobjectMap, None, g)):
                if (o2, rrparentTriplesMap, None, g) in ds:
                    parenttm = None
                    join_conditions = []
                    for s3, p3, o3, g3 in ds.quads((o2, None, None, g)):
                        if p3 == rrparentTriplesMap:
                            parenttm = g3.n3()[1:-1] + '_' + o3.n3()[1:-1]
                        elif p3 == rrjoinCondition:
                            parent = None
                            child = None
                            for s4, p4, o4, g4 in ds.quads((o3, None, None, g)):
                                if p4 == rrparent:
                                    parent = o4.n3()[1:-1]
                                elif p4 == rrchild:
                                    child = o4.n3()[1:-1]
                            join_conditions.append((child, parent))

                    obj = ObjectReferenceMap(parenttm, join_conditions)
                    om = ObjectMap(objmap_id=o2.n3(), theobject=obj)
                    oms.append(om)
                else:
                    val = None
                    res = None
                    tt = None
                    infered_tt = None
                    dtype = None
                    lang = None
                    for s3, p3, o3, g3 in ds.quads((o2, None, None, g)):
                        if p3 == rrtemplate:
                            res = TripleMapType.TEMPLATE
                            val = o3.n3()[1:-1]
                        elif p3 == rmlreference or p3 == rrcolumn:
                            res = TripleMapType.REFERENCE
                            val = o3.n3()[1:-1]
                        elif p3 == rrconstant:
                            res = TripleMapType.CONSTANT
                            val = o3.n3()[1:-1]
                            if isinstance(o3, rdflib.URIRef):
                                infered_tt = TermType.IRI
                            elif isinstance(o3, rdflib.Literal):
                                infered_tt = TermType.Literal
                            else:
                                infered_tt = TermType.BNode
                        elif p3 == rrtermType:
                            tt = TermType.IRI if 'IRI' in o3.n3() else TermType.BNode \
                                if "BNode" in o3.n3() else TermType.Literal
                        elif p3 == rrlanguage:
                            tt = TermType.Literal
                            lang = o3.n3()[1:-1]
                        elif p3 == rrdatatype:
                            tt = TermType.Literal
                            dtype = o3.n3()[1:-1]
                    if tt is None:
                        tt = infered_tt
                    obj = TermMap(value=val, resource_type=res, term_type=tt)

                    om = ObjectMap(objmap_id=o2.n3(), theobject=obj, datatype=dtype, language=lang)
                    oms.append(om)

        return oms

    @staticmethod
    def get_predicate_obj_map(t, ds, g):

        pom = []
        for s, p, o, g1 in ds.quads((t, rrpredicateObjectMap, None, g)):
            if (o, rrpredicate, None, g) in ds:
                for s2, p2, o2, g2 in ds.quads((o, rrpredicate, None, g)):
                    pred = TermMap(value=o2.n3()[1:-1], resource_type=TripleMapType.CONSTANT, term_type=TermType.IRI)
                    pm = PredicateMap(o2.n3()[1:-1], pred)
                    oms = RML.get_object_maps(o, ds, g)
                    for om in oms:
                        pom.append((pm, om))

            else:
                for s2, p2, o2, g2 in ds.quads((o, rrpredicateMap, None, g)):
                    val = None
                    res = None
                    for s3, p3, o3, g3 in ds.triples((o2, None, None, g)):
                        if p3 == rrtemplate:
                            res = TripleMapType.TEMPLATE
                            val = o3.n3()[1:-1]
                        elif p3 == rmlreference or p3 == rrcolumn:
                            res = TripleMapType.REFERENCE
                            val = o3.n3()[1:-1]
                        elif p3 == rrconstant:
                            res = TripleMapType.CONSTANT
                            val = o3.n3()[1:-1]

                    pred = TermMap(value=val, resource_type=res, term_type=TermType.IRI)
                    pm = PredicateMap(o2.n3()[1:-1], pred)
                    oms = RML.get_object_maps(o, ds, g)
                    for om in oms:
                        pom.append((pm, om))

        return pom

    @staticmethod
    def get_subject_map(s, ds, g):
        smap_id = None
        rdf_types = []
        val = None
        res = None
        tt = TermType.IRI
        subj = None
        if (s, rrsubject, None, g) in ds:
            for s, p, o, g1 in ds.quads((s, rrsubject, None, g)):
                smap_id = o.n3()[1:-1]
                val = o.n3()[1:-1]
                res = TripleMapType.CONSTANT
                tt = TermType.IRI if isinstance(o, rdflib.URIRef) else TermType.BNode

                subj = TermMap(value=val, resource_type=res, term_type=tt)

                break  # exactly one subject map
        else:
            for s, p, o, g1 in ds.quads((s, rrsubjectMap, None, g)):
                smap_id = o.n3()[1:-1]
                for s2, p2, o2, g2 in ds.quads((o, None, None, g)):
                    if p2 == rrtermType:
                        tt = TermType.BNode if 'BNode' in o2.n3() else TermType.IRI
                    if p2 == rrclass:
                        rdf_types.append(o2.n3()[1:-1])
                    if p2 == rmlreference or p2 == rrcolumn or p2 == rrtemplate or p2 == rrconstant:
                        res = TripleMapType.CONSTANT if p2 == rrconstant else TripleMapType.REFERENCE \
                            if p2 == rmlreference or p2 == rrcolumn else TripleMapType.TEMPLATE
                        val = o2.n3()[1:-1]

                subj = TermMap(value=val, resource_type=res, term_type=tt)

                break  # exactly one subject map

        subjectmap = SubjectMap(smap_id=smap_id, subject=subj, rdf_types=rdf_types)

        return subjectmap

    @staticmethod
    def get_logical_source(o, ds, g):
        ds_desc = {}

        name = None
        dstype = None
        dbmstype = None
        liter = 'row'
        refform = None
        query = None
        tablename = None
        sqlversion = None

        for s2, p2, o2, g2 in ds.quads((o, None, None, g)):
            if p2 == rmlsource:
                name = o2.n3()[1:-1]
                for s3, p3, o3, g3 in ds.quads((o2, None, None, g)):
                    if 'jdbcDSN' in p3.n3():
                        ds_desc[p3.n3()] = o3.n3()[1:-1]
                    if 'jdbcDriver' in p3.n3():
                        ds_desc[p3.n3()] = o3.n3()[1:-1]
                        if 'mysql' in ds_desc[p3.n3()]:
                            dbmstype = DataSourceType.MYSQL
                        elif 'postgresql' in ds_desc[p3.n3()]:
                            dbmstype = DataSourceType.POSTGRES
                        elif 'microsoft' in ds_desc[p3.n3()]:
                            dbmstype = DataSourceType.SQLSERVER
                    if 'username' in p3.n3():
                        ds_desc[p3.n3()] = o3.n3()[1:-1]
                    if 'password' in p3.n3():
                        ds_desc[p3.n3()] = o3.n3()[1:-1]

                    if p3 == RDF.type:
                        dstype = o3.n3()[1:-1]
                        if dstype == 'http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#Database':
                            dstype = DataSourceType.D2RQ
                        elif dstype == 'http://www.w3.org/ns/dcat#Dataset':
                            dstype = DataSourceType.DCAT
                        elif dstype == 'http://www.w3.org/ns/sparql-service-description#Service':
                            dstype = DataSourceType.SPARQL_ENDPOINT
                        elif dstype == 'http://www.w3.org/ns/csvw#Table':
                            dstype = DataSourceType.CSVW
                        else:
                            dstype = dbmstype

            if p2 == rrsqlVersion:
                ds_desc[p2.n3()] = o2.n3()[1:-1]
                sqlversion = o2.n3()[1:-1]
            if p2 == rrtableName:
                tablename = o2.n3()[1:-1]
            if p2 == rmlquery:
                query = o2.n3()[1:-1]
            if p2 == rmliterator:
                liter = o2.n3()[1:-1]
            if p2 == rmlreferenceFormulation:
                refform = o2.n3()

        source = RMLSource(name, ds_desc=ds_desc)
        source.dstype = dstype
        source.dbmstype = dbmstype

        ls = LogicalSource(source=source, logical_iterator=liter, reference_formulation=refform, query=query,
                           table_name=tablename, sql_version=sqlversion)

        return ls


class RMLReader:

    def __init__(self, path, ds, map_type="f"):
        self.path = path
        self.dsid = ds
        self.map_type = map_type

    def read(self):
        pass

    def get_logical_source(self):
        ls_query = "?tm rml:logicalSource ?ls . \n\t\t" \
                   "?ls rml:source ?source " \
                   "OPTIONAL {?ls rml:iterator ?iterator . }\n\t\t" \
                   "OPTIONAL {?ls rr:sqlVersion ?sqlVersion. }\n\t\t" \
                   "OPTIONAL {?ls rml:query ?lcQuery .} \n\t\t" \
                   "OPTIONAL {?ls rr:tableName ?tableName. }\n\t\t" \
                   "OPTIONAL {?ls rml:referenceFormulation ?refForm . }\n\t\t"
        ls_query = prefixes + '\n' + " SELECT DISTINCT ?tm ?ls ?source  ?p ?o ?iterator ?sqlVersion ?lcQuery ?tableName ?refForm WHERE {\n\t\t" + ls_query + " }"

    def get_sources(self, source):
        source_query = '<' + source + "> ?p ?o . "
        source_query = " SELECT DISTINCT ?p ?o WHERE {\n\t\t" + source_query + " }"

    def get_subjectmaps(self, triplemap_url):
        subj_query = "<" + triplemap_url + "> rml:logicalSource ?ls ." \
                                           "OPTIONAL { <" + triplemap_url + "> rr:subject ?subject . }" \
                                                                            "OPTIONAL { <" + triplemap_url + "> rr:subjectMap ?sm . " \
                                                                                                             "          " \
                                                                                                             "          OPTIONAL { ?sm rr:termType ?smtype . }" \
                                                                                                             "          OPTIONAL { ?sm rr:template ?smtemplate .}" \
                                                                                                             "          OPTIONAL { ?sm rr:constant ?constsubject .}" \
                                                                                                             "          OPTIONAL { ?sm rml:reference ?smreference .}"

        subj_query += " OPTIONAL { ?sm rr:class ?rdfmt .} }"

        subj_query = prefixes + '\n' + " SELECT DISTINCT * \n WHERE {\n\t\t" + subj_query + " }"

    def get_pred_obj_maps(self, triplemap_url):
        pass

    def get_predicate_maps(self, triplmap_url):
        pass

    def get_object_maps(self, triplemap_url):
        pass

    def load_all(self, filename):
        import rdflib

        graph = rdflib.Graph()
        graph.load(filename)

        for s, p, o in graph:
            print(s, p, o)


class DataSourceType(Enum):
    SPARQL_ENDPOINT = "SPARQL_Endpoint"
    MONGODB = "MongoDB"
    MONGODB_LD_FLAT = "MONGODB_LD_FLAT"
    NEO4J = "Neo4j"
    MYSQL = "MySQL"
    POSTGRES = "Postgres"
    SQLSERVER = 'SQLServer'

    SPARK_CSV = "SPARK_CSV"
    SPARK_TSV = "SPARK_TSV"
    SPARK_JSON = "SPARK_JSON"
    SPARK_XML = "SPARK_XML"

    HADOOP_CSV = "HADOOP_CSV"
    HADOOP_TSV = "HADOOP_TSV"
    HADOOP_JSON = "HADOOP_JSON"
    HADOOP_XML = "HADOOP_XML"

    REST_SERVICE = "REST_Service"

    LOCAL_CSV = "LOCAL_CSV"
    LOCAL_TSV = "LOCAL_TSV"
    LOCAL_JSON = "LOCAL_JSON"
    LOCAL_XML = "LOCAL_XML"
    LOCAL_RDF = "LOCAL_RDF"

    LOCAL_FOLDER = "LOCAL_FOLDER"
    SPARK_FOLDER = "SPARK_FOLDER"
    HADOOP_FOLDER = "HADOOP_FOLDER"

    CSV = "CSV"
    TSV = "TSV"
    XML = "XML"
    JSON = "JSON"
    RDF = "RDF"

    DCAT = 'DCat'
    D2RQ = 'D2RQ'
    CSVW = 'CSVW'

    def __repr__(self):
        return self.value

    def __str__(self):
        return self.value
