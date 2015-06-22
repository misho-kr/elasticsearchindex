"""
Elasticsearch Index backend class for the Docker registry
"""

import logging
import traceback
import time
import json

from elasticsearch import (
    Elasticsearch, RoundRobinSelector, ImproperlyConfigured, ElasticsearchException,
    SerializationError, TransportError, NotFoundError, ConflictError, RequestError,
    ConnectionError
)

from docker_registry import storage
from docker_registry.lib import config
from docker_registry.lib import signals
from docker_registry.lib.index import Index

logger = logging.getLogger(__name__)
cfg = config.load()

from . import ZookeeperTransport

class Index (Index):
    """A backend for the search endpoint

    The backend can use .walk_storage to generate an initial index,
    ._handle_repository_* to stay up to date with registry changes,
    and .results to respond to queries.
    """

    # default settings
    DEFAULT_ES_PARAMS = {
        'sniff_on_connection_fail': True,
        'timeout':                  10,
        'max_retries':              5,
        'sniff_timeout':            1,
        'sniffer_timeout':          20,
        'selector_class':           RoundRobinSelector,
        'transport_class':          ZookeeperTransport.ZookeeperTransport
    }

    def __init__(self, **kwargs):

        params = Index.DEFAULT_ES_PARAMS.copy()
        params.update({ (k,cfg.search_options[k]) for k in cfg.search_options.keys() if k in params.keys() })
        params.update({ (k,v) for k,v in kwargs.items() if k in params.keys() })

        # don't pass "timeout" params to Elasticsearch construct,
        # the name is too generic and the kwargs are passed to a bunch of
        # constructors, there may be inadvertent name clashes
        self.timeout = params["timeout"]
        del params["timeout"]

        try:
            self.es = Elasticsearch(**params)
            logger.info("created ES-backed Index, max-retries=%d, timeout=%d",
                        params["max_retries"], self.timeout)
        except:
            self.es = None
            logger.error('*****Elasticsearch Exception: Unable to initiate connection' + traceback.format_exc());

        #Subscribe to the tag creation event
        signals.tag_created.connect(self._handle_tag_created)
        super(Index, self).__init__()

    def _walk_storage(self, store):
        logger.debug("Request to walk index storage, not implemented")

    def _handle_repository_created(self, sender, namespace, repository, value):
        logger.debug("Repository created, pass")

    def _handle_tag_created(self, sender, namespace, repository, tag, value):
        """
        Triggered after a new tag creation operation has completed

        @type  sender: List
        @param sender: Flask object with request details
        @type  namespace: String
        @param namespace: Docker namespace under which the image layers are stored
        @type  repository: String
        @param repository: Docker repository under which the image layers are stored
        @type  value: List
        @param value: List of Docker image layer JSON metadata objects
        """

        if (self.es != None):
            #Load the tag image layer data from the data store
            store = storage.load()
            data = store.get_content(store.image_json_path(value))
            data = json.loads(data)

            #Manufacture the tag document to be writen into the index
            document={}

            #Concatenate the <namespace>_<repository>_imageid to generate a unique primary key id
            document['id'] = namespace + '_' + repository + '_' + tag
            document['namespace'] = namespace
            document['repository'] = repository
            document['tag'] = tag
            document['checksum'] = value
            document['description'] = data.get('author', '')
            self.set_in_index(document, cfg.search_options.index, 'tags')
        else:
            logger.error('*****Elasticsearch Exception: Unable to store Tag metadata for ' + namespace + '/' + repository + ' -- ' + traceback.format_exc());

    def _handle_repository_updated(self, sender, namespace, repository, value):
        """
        Triggered after a docker push operation has completed via Signals

        @type  sender: List
        @param sender: Flask object with request details
        @type  namespace: String
        @param namespace: Docker namespace under which the image layers are stored
        @type  repository: String
        @param repository: Docker repository under which the image layers are stored
        @type  value: List
        @param value: List of Docker image layer JSON metadata objects
        """
        if (self.es != None):

            #Get the list of checksums for all the image layers which will be included in the parent layer
            checkSums = self.get_checksums(value)

            store = storage.load()

            #Reverse the checksums list since the initial layer checksum seems to mostly be at the end
            #and it reduces the number of iterations
            value.reverse()

            #Loop over the image layers in this repository and collect the metadata
            for item in value:

                #Load the image layer data from the data store
                data = store.get_content(store.image_json_path(item['id']))
                data = json.loads(data)

                #Generate a single index document for this image also containing the list of image checksums
                if (data.get('parent') == None):
                    document = self.create_index_document(data, checkSums, namespace, repository)
                    self.set_in_index(document, cfg.search_options.index, 'images')
        else:
            logger.error('*****Elasticsearch Exception: Unable to store Image metadata for ' + namespace + '/' + repository + ' -- ' + traceback.format_exc());

    def _handle_repository_deleted(self, sender, namespace, repository):
        pass

    def results(self, search_term):
        pass

    def create_index_document(self, data, checkSums, namespace, repository):

        document={}

        #Concatenate the <namespace>_<repository>_imageid to generate a unique primary key id
        document['id'] = namespace + "_" + repository
        document['namespace'] = namespace
        document['repository'] = repository
        document['imageLayers'] = checkSums
#        document['parent'] = data.get('parent',"")
#        document['created'] = data.get('created',"")
#        document['container'] = data.get('container',"")
#        document['author'] = data.get('author',"")
#        document['architecture'] = data.get('architecture',"")
#        document['os'] = data.get('os',"")
#        document['size'] = data.get('Size',"")
#        document['comment'] = data.get('comment',"")
#        document['hostname'] = data.get('container_config').get('Hostname',"")
#        document['cmd'] = data.get('container_config').get('Cmd',"")
#        document['domainName'] = data.get('container_config').get('Domainname',"")
#        document['user'] = data.get('container_config').get('User',"")
#        document['memory'] = data.get('container_config').get('Memory',"")
#        document['memorySwap'] = data.get('container_config').get('MemorySwap',"")
#        document['cpuShares'] = data.get('container_config').get('CpuShares',"")
#        document['attachStdin'] = data.get('container_config').get('AttachStdin',"")
#        document['attachStdout'] = data.get('container_config').get('AttachStdout',"")
#        document['attachStderr'] = data.get('container_config').get('AttachStderr',"")
#        document['portSpecs'] = data.get('container_config').get('PortSpecs',"")
        #document['exposedPorts'] = data.get('container_config').get('ExposedPorts',"")
#        document['tty'] = data.get('container_config').get('Tty',"")
#        document['openStdin'] = data.get('container_config').get('OpenStdin',"")
#        document['stdinOnce'] = data.get('container_config').get('StdinOnce',"")
#        document['dns'] = data.get('container_config').get('Dns',"")
#        document['image'] = data.get('container_config').get('Image',"")
#        document['volumes'] = data.get('container_config').get('Volumes',"")
#        document['volumesFrom'] = data.get('container_config').get('VolumesFrom',"")
#        document['workingDir'] = data.get('container_config').get('WorkingDir',"")
#        document['entrypoint'] = data.get('container_config').get('Entrypoint',"")
#        document['networkDisabled'] = data.get('container_config').get('NetworkDisabled',"")
        #document['onBuild'] = data.get('container_config').get('OnBuild',"")
#        document['domainName'] = data.get('container_config').get('Domainname',"")
#        document['dockerVersion'] = data.get('docker_version',"")
        return document

    def get_checksums(self, list):
        """
        Extract the list of checksums

        @type  list: List
        @param list: List of image layer objects
        @return:  List of checksums
        """
        checkSums = []
        for item in list:
            checkSums.append(item['id'])
        return checkSums

    def set_in_index(self, document, index, type):
        """
        Store the list of documents in the Elasticsearch index via HTTP APIs

        @type  document: List
        @param document: JSON document
        """
        response = None

        # The ElasticSearch Transport will retry <<max-retries> times to write,
        # each attempt will go to different node in RoundRobin order
        try:
            document['epoch'] = int(time.time())
            response = self.es.index(
                                index=index,
                                doc_type=type,
                                id=document['id'],
                                body=document,
                                request_timeout=self.timeout)
        except (ImproperlyConfigured,
                ElasticsearchException,
                TransportError,
                NotFoundError,
                ConflictError,
                RequestError,
                SerializationError,
                ConnectionError,
                Exception) as e:
            logger.warning("ES index write failed: {0}! ".format(e.__class__.__name__) + \
                           traceback.format_exc())
        else:
            logger.debug("ES index write successful")

        return(response is not None and response != '')
