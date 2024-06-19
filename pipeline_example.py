import argparse
import apache_beam as beam
from apache_beam.transforms.external import JavaExternalTransform
import typing


# # Command line arguments
parser = argparse.ArgumentParser(description='Load DToL data from string into Elasticsearch')
parser.add_argument('--address', required=True, help='Elasticsearch URL')
parser.add_argument('--index', required=True, help='Elasticsearch index name')
parser.add_argument('--username', required=True, help='Elasticsearch username')
parser.add_argument('--password', required=True, help='Elasticsearch password')
parser.add_argument('--expansion_service_port', required=True, help='Java expansion service port')

opts = parser.parse_args()

inputs = [
    'dummy_string_1',
    'dummy_string_2',
    'dummy_string_3'
]

ConnectionConfig = typing.NamedTuple(
    'ConnectionConfig', [('address', str), ('index', int), ('username', str), ('password', str)])

con_config = ConnectionConfig(address=opts.address, index=opts.index, username=opts.username, password=opts.password)

write_to_elastic_transform = JavaExternalTransform(
            'org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Write',
            expansion_service=('localhost:%s' % opts.expansion_service_port)
        ).withConnectionConfiguration(con_config)

with beam.Pipeline() as p:
    (p
     | 'ReadStrings' >> beam.Create(inputs)
     | 'WriteToElastic' >> write_to_elastic_transform
     )

