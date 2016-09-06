#!/usr/bin/env python
# -*- coding: utf-8 -*-


from lxml import etree
from multiprocessing.dummy import Pool
from oaipmh.client import Client
from oaipmh.error import BadVerbError
from oaipmh.error import NoRecordsMatchError
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
from os.path import exists
from os import makedirs
from requests.exceptions import HTTPError
from time import time, ctime
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import tostring
import logging
import HTMLParser
import re
import urllib

__author__ = 'Gustavo Siqueira'

FILE_ESCAPE_CHARS = r'\]\[\/\\\;\,\>\<\&\*\:\%\=\+\@\!\#\^\(\)\|\?\^'
METADATA = 'oai_dc'
ENCODE = 'utf-8'
SEPARATOR = '================================'

#TODO Usar datestamp para coletas retroativas.
class OAICrawler():
    '''
        This class provides a basic OAI crawler for repositories that use such a protocol.
    '''

    def __init__(self):
        logging.basicConfig(filename='./crawler.log',
                            filemod='w',
                            level=logging.DEBUG,
                            format='[%(levelname)s] (%(threadName)s) -   %(message)s')
        self.logger = logging.getLogger(__name__)
        self.unvisited_repository = list()

    def pool_worker(self, repository_list, threads=4):
        start_time = time()
        self.logger.info('Starting process at {0}.'.format(ctime()))

        pool = Pool(threads)
        pool.imap_unordered(self.retrieval, repository_list)
        pool.close()
        pool.join()

        self.logger.info(u'Process done at {0}. Total time: {1} seconds'.format(ctime(), time() - start_time))

    def retrieval(self, repository):
        self.logger.info(u'Trying to retrieve url {0}'.format(repository[1]).encode(ENCODE))

        registry = MetadataRegistry()
        registry.registerReader(METADATA, oai_dc_reader)

        try:
            client = Client(repository[1], registry)

            self.logger.info(SEPARATOR)
            self.logger.info(u'Connection established successfully...')

            # identify info
            identify = client.identify()
            repository_name = identify.repositoryName()
            repository_name_normalized = re.sub(re.compile(FILE_ESCAPE_CHARS), '', repository_name).strip() \
                .replace(' ', '_').lower()
            base_url = identify.baseURL().encode(ENCODE)
            protocol_version = identify.protocolVersion().encode(ENCODE)
            granularity = identify.granularity().encode(ENCODE)
            compression = identify.compression()
            deleted_record = identify.deletedRecord().encode(ENCODE)

            metadata = {'repository_name': repository_name,
                        'base_url': base_url,
                        'latest_url': repository[1],
                        'protocol_version': protocol_version,
                        'granularity': granularity,
                        'compression': str(compression).strip('[]'),
                        'deleted_record': deleted_record}

            self.logger.info(u'Repository name: {0}'.format(repository_name))
            self.logger.info(u'URL connected: {0}'.format(repository[1]))
            self.logger.info(u'Base URL: {0}'.format(base_url))
            self.logger.info(u'Protocol version: {0}'.format(protocol_version))
            self.logger.info(u'Granularity: {0}'.format(granularity))
            self.logger.info(u'Compression: {0}'.format(compression))
            self.logger.info(u'Deleted record: {0}'.format(deleted_record))

            records_count = 0
            deleted_count = 0
            records_list = list()
            parsed_records_list = list()

            # we're not interested in all sets, so we must iterate over the ones we have and want to crawl
            if repository[2] is not None:
                self.logger.info(u'Fetching set {0}...'.format(repository[2]))
                records_list = client.listRecords(metadataPrefix=METADATA, set=repository[2])
            else:
                records_list = client.listRecords(metadataPrefix=METADATA)
            if records_list is not None:
                for record in records_list:
                    records_count += 1
                    if record[0].isDeleted():
                        deleted_count += 1
                    if record[1] is not None:
                        parsed_records_list.append(tostring(record[1].element()))
                self.logger.info(
                    u'Retrieved {0} records from set {1} where {2} were deleted'.format(records_count, repository[2],
                                                                                       deleted_count))
            if not exists(''.join(['files/', repository_name_normalized, '/'])):
                self.logger.info('Creating storage folder for {0}...'.format(repository_name))
                makedirs(''.join(['files/', repository_name_normalized, '/']))

            self.logger.info(u'Creating storage files...')
            meta_file = open(''.join(['files/', repository_name_normalized, '/metadata.xml']), 'w')
            metadata['records_number'] = records_count
            metadata['deleted_number'] = deleted_count
            meta_file.write(tostring(dict_to_xml('metadata', metadata)))
            meta_file.close()

            record_file = open(''.join(
                ['files/', repository_name_normalized, '/', repository_name_normalized, '_', repository[2], '.xml']),
                'w')
            record_file.write(''.join(parsed_records_list))
            record_file.close()

        except NoRecordsMatchError, nrme:
            self.logger.error(u'{0} on repository {1}'.format(nrme.message, repository_name))

            # add url to unvisited_url and ask retrieval to try to crawl them again
            if nrme.message == 'No matches for the query':
                self.unvisited_repository.append(repository)

        except BadVerbError, bve:
            self.logger.error(u'{0}. Check repository {1}'.format(bve.message, repository[0]))

        except HTTPError, httpe:
            self.logger.error(
                u'Error 404. Page not found. Check url {0} for repository {1}'.format(repository[1], repository[0]))

        except urllib.error.URLError, urle:
            self.logger.error(u'Bad URL: {0}. Check if this repository really exists.'.format(repository[1]))

        except etree.XMLSyntaxError, xmlse:
            self.logger.error(u'Something went wrong with response XML for repository {0}'.format(repository[0]))

        except Exception, e:
            # if any unexpected error occurs, we must keep tracking
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

            # parsing database names with encoded characters
            line_components[0] = HTMLParser.HTMLParser().unescape(line_components[0])

            # parsing sets
            if line_components[2] != 'None':
                line_components[2] = line_components[2].split('=')[1]
            # else:
            #    line_components[2] = None

            # parsing metadata_prefix
            line_components[3] = line_components[3].split('=')[1]

            parsed_repository_list.append(line_components)
    return parsed_repository_list


if __name__ == '__main__':
    # Main usage
    # OAICrawler().pool_worker(parse_repository_file('world_repositories'))

    # Test usage
    OAICrawler().pool_worker([['AUT University Doctoral Theses', 'http://aut.researchgateway.ac.nz/dspace-oai/request',
                               'col_10292_4', 'mets']])
