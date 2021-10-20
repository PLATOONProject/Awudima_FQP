#!/usr/bin/env python3

from distutils.core import setup
from setuptools import find_packages

setup(name='awudima-fqp',
      version='0.3',
      description='Awudima - Federated Query Processing over Semantic Data Lakes',
      author='Kemele M. Endris',
      author_email='kemele.endris@gmail.com',
      url='https://github.com/Awudima/',
      scripts=['./start_endpoint.sh'],
      packages=find_packages(exclude=['docs']),
      install_requires=["ply==3.11",
                        "flask==2.0.2",
                        "requests==2.26.0",
                        'hdfs==2.6.0',
                        'pyspark==3.1.2',
                        'pymongo==3.12.0',
                        'mysql-connector-python==8.0.26',
                        'neo4j-driver==4.3.6',
                        'rdflib==5.0.0',
                        'networkx==2.5.1',
                        'pydrill==0.3.4',
                        'SPARQLWrapper==1.8.5'],
      include_package_data=True,
      license='GNU/GPL v2'
      )