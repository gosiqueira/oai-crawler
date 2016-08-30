#!/usr/bin/env python
# -*- coding: utf-8 -*-


from multiprocessing.dummy import Pool
from oaipmh.client import Client
from oaipmh.error import NoRecordsMatchError
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
from os.path import exists
from os import makedirs
from time import time, ctime
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import tostring
import logging


__author__ = 'Gustavo Siqueira'


METADATA = 'oai_dc'
ENCODE = 'utf-8'
SEPARATOR = '================================'


class OAICrawler():
    '''
        This class provides a basic OAI crawler for repositories that use such a protocol.
    '''
    def __init__(self):
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(__name__)
        self.unvisited_repository = list()


    def pool_worker(self, repository_list, threads=4):
        start_time = time()
        self.logger.info('Startinf process at {0}.'.format(ctime()))

        pool = Pool(threads)
        pool.map(self.retrieval, repository_list)
        pool.close()
        pool.join()

        self.logger.info('Process done at {0}. Total time: {1} seconds'.format(ctime(), time() - start_time))


    def retrieval(self, repository):
        self.logger.info('Trying to retrieve url {0}'.format(repository[1]).encode(ENCODE))

        registry = MetadataRegistry()
        registry.registerReader(METADATA, oai_dc_reader)

        try:
            client = Client(repository[1], registry)

            self.logger.info(SEPARATOR)
            self.logger.info('Connection established successfully...')

            # identify info
            identify = client.identify()
            repository_name = identify.repositoryName()
            base_url = identify.baseURL().encode(ENCODE)
            protocol_version = identify.protocolVersion().encode(ENCODE)
            granularity = identify.granularity().encode(ENCODE)
            compression = identify.compression()
            deleted_record = identify.deletedRecord().encode(ENCODE)

            metadata = {'repository_name': repository_name,
                        'base_url': base_url,
                        'protocol_version': protocol_version,
                        'granularity': granularity,
                        'compression': str(compression).strip('[]'),
                        'deleted_record': deleted_record}

            self.logger.info('Repository name: {0}'.format(repository_name))
            self.logger.info('Base URL: {0}'.format(base_url))
            self.logger.info('Protocol version: {0}'.format(protocol_version))
            self.logger.info('Granularity: {0}'.format(granularity))
            self.logger.info('Compression: {0}'.format(compression))
            self.logger.info('Deleted record: {0}'.format(deleted_record))

            if not exists(''.join(['files/', repository[0], '/'])):
                self.logger.info('Creating storage folder for {0}...'.format(repository_name))
                makedirs(''.join(['files/', repository[0], '/']))

            self.logger.info('Creating metadata file...')
            meta_file = open(''.join(['files/', repository[0], '/metadata.xml']), 'w')
            meta_file.write(tostring(dict_to_xml('metadata', metadata)))
            meta_file.close()

            record_count = 0
            deleted_count = 0
            records = list()

            # we're not interested in all sets, so we must iterate over the ones we want to crawl
            if repository[2] != 'None':
                self.logger.info('Fetching set {0}...'.format(repository[2]))
                for record in client.listRecords(metadataPrefix=METADATA, set=repository[2]):
                    record_count += 1
                    if record[0].isDeleted():
                        deleted_count += 1
                    records.append(tostring(record[1].element()))
                self.logger.info(
                    'Retrieved {0} records from set {1} where {2} were deleted'.format(record_count, repository[2],
                                                                                       deleted_count))
            else:
                for record in client.listRecords(metadataPrefix=METADATA, set=repository[2]):
                    record_count += 1
                    if record[0].isDeleted():
                        deleted_count += 1
                    records.append(tostring(record[1].element()))
                self.logger.info(
                    'Retrieved {0} records from set {1} where {2} were deleted'.format(record_count, repository[2],
                                                                                       deleted_count))
            record_file = open(''.join(['files/', repository[0], '/', repository[0], '.xml']), 'w')
            record_file.write(''.join(records))
            record_file.close()

        except NoRecordsMatchError, nrme:
            self.logger.error('{0} on repository {1}'.format(nrme.message, repository_name))

            # add url to unvisited_url and ask retrieval to try to crawl them again
            if nrme.message == 'No matches for the query':
                self.unvisited_repository.append(repository)

        # if any unexpected error occurs, we must keep tracking
        except Exception, e:
            self.logger.error(e.message, exc_info=True)


def dict_to_xml(tag, d):
    '''
    Turn a simple dict of key/value pairs into XML
    '''
    elem = Element(tag)
    for key, val in d.items():
        child = Element(key)
        child.text = str(val)
        elem.append(child)
    return elem


def parse_repository_file(file):
    '''
        This function provides a parse for the repositories file.
    '''
    parsed_repository_list = list()
    with open(file, 'r') as rep_file:
        for line in rep_file:
            line_components = line.strip().split('\t')
            if line_components[2] != 'None':
                line_components[2] = line_components[2].split('=')[1]
            line_components[3] = line_components[3].split('=')[1]
            parsed_repository_list.append(line_components)
    return parsed_repository_list


if __name__ == '__main__':
    # Main usage
    #OAICrawler().pool_worker(parse_repository_file('world_repositories'))

    # Test usage
    OAICrawler().pool_worker([['AUT University Doctoral Theses','http://aut.researchgateway.ac.nz/dspace-oai/request','col_10292_4','mets']])
