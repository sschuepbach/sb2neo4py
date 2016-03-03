import gzip
from json import loads
import argparse
from os import walk
from py2neo import authenticate, Graph, watch
import logging


ap = argparse.ArgumentParser(description='Extracts certain relations in linked-swissbib workflow dumps and models'
                                         'them in a Neo4j graph database.')
ap.add_argument('rootdir', type=str, help='Root dump directory')
ap.add_argument('-l', dest='logfile', required=True, help='Path to logfile')
a = ap.parse_args()

logging.basicConfig(filename=a.logfile, level=logging.INFO)
watch(logger_name='py2neo.cypher', level=logging.WARN)
watch(logger_name='httpstream', level=logging.WARN)


def filelistbuilder(rootdir: str) -> list:
    fl = list()
    for dirname, subdirlist, filelist in walk(rootdir):
        fl.extend([dirname + '/' + f for f in filelist])
    return fl


def extract(json, type: str) -> str:
    statement = ''
    if type == 'bibliographicResource':
        # Build resource node and graphs to contributors
        resid = json['@id']
        statement = 'MERGE (br:bibliographicResource { name: "' + resid + '", active:"true"})'
        if 'dct:contributor' in json:
            contribids = json['dct:contributor']
            counter = 0
            for id in contribids:
                counter += 1
                if id.count('person') > 0:
                    label = 'person'
                else:
                    label = 'organisation'
                statement += ' MERGE (cont' + str(counter)  + ':' + label\
                             + ' { name: "' + id + '" }) CREATE (br)-[:contributor]->(cont' + str(counter) + ')'
    elif type == 'person':
        # Build person node
        persid = json['@id']
        statement = 'MERGE (p:person { name: "' + persid +'"})'
    elif type == 'organisation':
        # Build organisation node
        orgid = json['@id']
        statement = 'MERGE (o:organisation { name: "' + orgid + '" })'
    elif type == 'item':
        # Build item node and graph to resource
        itemid = json['@id']
        resid = json['bf:holdingFor']
        statement = 'MERGE (i:item { name: "' + itemid + '" }) MERGE (br:bibliographicResource { name: "'\
                    + resid + '", active: "true"}) ' \
                    'CREATE (i)-[:itemof]->(br)'
    elif type == 'document':
        # Build graphs for bf:local to resource
        resid = json['@id'][:-6]
        local = json['bf:local']
        statement = 'MERGE (br:bibliographicResource { name: "' + resid + '", active: "true"})'
        counter = 0
        for loc in local:
            counter += 1
            statement += ' MERGE (l' + str(counter) + ':localsignature { name: "' + loc + '" }) CREATE (l' \
                         + str(counter) + ')-[:signatureof]->(br)'
    return statement


fl = filelistbuilder(a.rootdir)
authenticate('localhost:7474', 'neo4j', 'neo4j_15')
graph = Graph()
tx = graph.cypher.begin()

for elem in fl:
    if elem.endswith('jsonld.gz'):
        logging.info('Processing file {}'.format(elem))
        with gzip.open(elem, 'rb') as f:

            counter = 0
            type = ''
            for l in f:
                counter += 1
                if counter == 1:
                    type = loads(bytes.decode(l, 'utf_8'))['index']['_type']
                elif counter % 2 == 0:
                    tx.append(extract(loads(bytes.decode(l, 'utf_8')), type))
                    tx.process()
tx.commit()
