
__author__ = "Kemele M. Endris"

from enum import Enum
from typing import Dict, List

from SPARQLWrapper import SPARQLWrapper, SPARQLWrapper2, JSON, JSONLD, XML, RDFXML


class SPARQLEndpointWrapper:

    @staticmethod
    def contact_sparql_endpoint(query, endpoint):
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        if "results" in results:
            for x in results['results']['bindings']:
                for key, props in x.items():
                    # Handle typed-literals and language tags
                    suffix = ''
                    if 'type' not in props:
                        continue

                    if props['type'] == 'typed-literal':
                        if isinstance(props['datatype'], bytes):
                            suffix = "^^<" + props['datatype'].decode('utf-8') + ">"
                        else:
                            suffix = "^^<" + props['datatype'] + ">"
                    elif "xml:lang" in props:
                        suffix = '@' + props['xml:lang']
                    try:
                        if isinstance(props['value'], bytes):
                            if props['type'] == 'uri':
                                props['value'] = props['value'].decode('utf-8').replace('\\', "")
                            x[key] = props['value'] + suffix

                        else:
                            if props['type'] == 'uri':
                                props['value'] = props['value'].replace('\\', "")

                            x[key] = props['value'] + suffix
                    except:
                        x[key] = props['value'] + suffix

                    if isinstance(x[key], bytes):
                        x[key] = x[key].decode('utf-8')

            reslist = results['results']['bindings']
            return reslist, len(reslist)
        elif 'boolean' in results:
            return results['boolean'], 1

        return [], -2
