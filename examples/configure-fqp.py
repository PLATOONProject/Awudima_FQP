from awudima import Federation, DataSource, DataSourceType


def configure_from_json_config_file(json_config_file):
    """
    Content of the config file should have the following properties:
        {
          "fedId": "ID of the federation/data integration project",
          "name": "Name of the Federation/data integration project",
          "desc": "More detailed description can be added here!",
          "sources": {
            "ID_of_1st_data_source": {
              "dsId": "ID_of_1st_data_source",
              "name": "Name of the data source",
              "desc": "More description of the data set can be added here!",
              "url": "IPAddress:Port or fully qualified URL of this data source",
              "dstype": "MONGODB_LD_FLAT", # OR one of the following [MySQL, SPARQL_Endpoint, MongoDB, Neo4j, ..]
              "params": {
                "<http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#jdbcDSN>": "name_of_database - if dstype=MONGODB_LD_FLAT/MySQL/MongoDB, etc",
                "username": "root",
                "password": "1234"
              },
              "acronym": "Acronym of the data source, if any",
              "labeling_property": "http://www.w3.org/2000/01/rdf-schema#label",
              "mapping_paths": [],  # Path to RML files .ttl or any RDF serialization format
              "mappings_type": null, # OR 'RML file'
              "typing_predicate": "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
            } # , ADD More data sources to federate
          }
        }
    :param json_config_file:
    :return:
    """

    fed = Federation.config(json_config_file)
    return fed


def configure_using_python_object():
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

    return fed


if __name__ == '__main__':

    # If configuration is stored in JSON file
    fed = configure_from_json_config_file("sample-config.json")

    # Configuration can also be done using Python API from awudima pyrdfmt package
    # fed = configure_using_python_object()

    from pprint import pprint
    # inspect the config obj as json
    pprint(fed.to_json())

    # Extract additional metadata - aka - RDF Molecule Templates (RDFMTs)
    fed.extract_molecules()
    # Save it to file, as JSON, so that we can load this configuration with RDFMTs later
    fed.dump_to_json('federation_config.json')
    pprint(fed.to_json())
