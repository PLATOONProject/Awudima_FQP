
from awudima import Federation, DataSource, DataSourceType
from awudima import AwudimaFQP


if __name__ == '__main__':

    # If configuration is stored in JSON file
    # fed = Federation.config("sample-config.json")

    # Configuration can also be done using Python API from awudima pyrdfmt package
    fed = Federation(fedId="ID of the federation/data integration project",
                     name="Name of the Federation/data integration project",
                     desc="More detailed description can be added here!")

    ds1 = DataSource(dsId="ID_of_1st_data_source",  # name of the database
                     name='Name of the data source',  # Id of the data source
                     url='0.0.0.0:27017',  # IPAddress:Port or fully qualified URL of this data source
                     dstype=DataSourceType.MONGODB_LD_FLAT,  # type of the data source
                     desc="More description of the data set can be added here!",
                     params={
                         'username': 'root',
                         'password': '1234',
                         "<http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#jdbcDSN>": "name_of_database - if dstype=MONGODB_LD_FLAT/MySQL/MongoDB, etc"
                     },
                     acronym='Acronym of the data source, if any')

    # Add data source 1 to the federation configuration
    fed.addSource(ds1)

    # Add more data sources as desired

    from pprint import pprint
    # inspect the config obj as json
    pprint(fed.to_json())

    # Extract additional metadata - aka - RDF Molecule Templates (RDFMTs)
    fed.extract_molecules()
    # Save it to file, as JSON, so that we can load this configuration with RDFMTs later
    fed.dump_to_json('federation_config.json')
    pprint(fed.to_json())

    # Create Executor from the configuration object 'fed'
    fqp = AwudimaFQP(fed)

    query = """
            prefix cim: <http://www.iec.ch/TC57/CIM#>
            prefix plt: <https://w3id.org/platoon/>
            prefix seas: <https://w3id.org/seas/> 
            prefix wgs84_pos: <http://www.w3.org/2003/01/geo/wgs84_pos#>
            prefix qudt: <http://www.qudt.org/2.1/schema/qudt/>
            prefix time: <http://www.w3.org/2006/time#>
            SELECT DISTINCT *
            WHERE{            
                ?tcontext <http://www.w3.org/2006/time#inXSDDateTime> ?inxsdtime.
                FILTER (?inxsdtime >= xsd:dateTime("2021-06-07T17:35:19.868869Z") && ?inxsdtime <= xsd:dateTime("2021-06-07T19:35:19.868869Z") )
            }
        """

    # Inspect the source selection
    pprint(fqp.get_sources_selected(query))

    # Inspect the decomposition of the query after sources are selected
    print(fqp.get_decompose_query(query))

    # Inspect the physical plan created to execute the query
    print(fqp.get_physical_plan(query))

    # EXECUTE query - source selection, decomposition, planning and executing the given query at once
    # and return results as SPARQL JSON Result Format
    resultSet = fqp.execute(query, keep_in_memory=True)

    # iterate results as raw dict obj
    for row in resultSet.get():
        print(row)

    # OR get SPARQL-JSON Result formated json object
    results = resultSet.results

    print(results)

    # Inspect the decomposition of the query
    print(resultSet.decomposition)

    # Inspect the plan of the query
    print(resultSet.plan)
