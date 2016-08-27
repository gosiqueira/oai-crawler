from oaipmh.client import Client
from oaipmh.error import NoRecordsMatchError
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
from os.path import exists
from os import makedirs
from time import time, ctime
import logging
import unicodedata


__author__ = 'Gustavo Siqueira'

ENCODE = 'ASCII'
METADATAPREFIX = 'oai_dc'
SEPARATOR = '================================'


'''
This class provides a basic OAI crawler for repositories that use such a protocol.
'''
class OAICrawler():

    # constructor
    def __init__(self):
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(__name__)
        self.nonvisited_url = list()

    # main method
    def retrieval(self, url_list):
        start_time = time()
        self.logger.info('Starting crawling process at {0}'.format(ctime()))

        # TODO transform url_list into a url_map or something related which we can have a pair <url, set>
        for url in url_list:
            self.logger.info('Trying to retrieve url {0}'.format(url).encode(ENCODE))

            registry = MetadataRegistry()
            registry.registerReader(METADATAPREFIX, oai_dc_reader)

            try:
                client = Client(url, registry)

                self.logger.info(SEPARATOR)
                self.logger.info('Connection established successfully...')

                # identify info
                identify = client.identify()
                repository_name = remove_diacritic(identify.repositoryName())
                base_url = identify.baseURL().encode(ENCODE)
                protocol_version = identify.protocolVersion().encode(ENCODE)
                granularity = identify.granularity().encode(ENCODE)
                compression = identify.compression()
                deleted_record = identify.deletedRecord().encode(ENCODE)

                metadata = [repository_name, base_url, protocol_version, granularity, compression, deleted_record]

                self.logger.info('Repository name: {0}'.format(repository_name))
                self.logger.info('Base URL: {0}'.format(base_url))
                self.logger.info('Protocol version: {0}'.format(protocol_version))
                self.logger.info('Granularity: {0}'.format(granularity))
                self.logger.info('Compression: {0}'.format(compression))
                self.logger.info('Deleted record: {0}'.format(deleted_record))

                if not exists(''.join(['files/', repository_name, '/'])):
                    self.logger.info('Creating storage folder for {0}...'.format(repository_name))
                    makedirs(''.join(['files/', repository_name, '/']))

                self.logger.info('Creating metadata file...')
                meta_file = open(''.join(['files/', repository_name, '/metadata.txt']), 'w')
                meta_file.write(self.build_metafile_xml(metadata))
                meta_file.close()

                record_count = 0
                deleted_count = 0

                #we're not interested in all sets, so we must iterate over the ones we want to crawl
                for set in url_list:
                    self.logger.info('Fetching set {0}...'.format(set))
                    for record in client.listRecords(metadataPrefix=METADATAPREFIX, set='col_10923_341'):
                        record_count += 1
                        if record[0].isDeleted():
                            deleted_count += 1
                            # TODO convert dict to default xml oai format
                            # TODO verify if the final xml encoding is correct
                            # TODO get oai dtd file
                        print record[1].getMap()

                    self.logger.info('Retrieved {0} records from set {1} where {2} were deleted'.format(record_count, set, deleted_count) )

            # if the client can't return records this error is thrown
            except NoRecordsMatchError, nrme:
                self.logger.error(nrme.message, exc_info=True)
                #add url to nonvisited_url and ask retrieval to try to crawl them again
                self.nonvisited_url = url
            # if any unexpected error occurs, we must keep tracking
            except Exception, e:
                self.logger.error(e.message, exc_info=True)

        self.logger.info('Process done at {0}. Total time: {1} seconds'.format(ctime(), time() - start_time))

    def build_metafile_xml(self, data):
        # TODO parse a array into a metadata xml or json
        return data.__str__()


'''
Accept a unicode string, and return a normal string without any diacritical marks.
'''
def remove_diacritic(input):
    return unicodedata.normalize('NFKD', input).encode(ENCODE, 'ignore')

if __name__ == '__main__':
    OAICrawler().retrieval(['http://repositorio.pucrs.br/oai/request'])
