#! /usr/bin/python

__author__="Ashish Hunnargikar"
__date__ ="$Jun 13, 2014 12:33:33 PM$"

import time
import os
import subprocess
from datetime import date, timedelta
from elasticsearch.transport import Transport
from elasticsearch import (Elasticsearch, RoundRobinSelector, ImproperlyConfigured, ElasticsearchException, 
    SerializationError, TransportError, NotFoundError, ConflictError, RequestError, ConnectionError)
import simplejson as json
from kazoo.client import KazooClient
from kazoo.exceptions import (KazooException)
os.environ['DEBUG'] = 'true'
#os.environ['CRON'] = '180'
#os.environ['DAYS'] = '1'
#
##Zookeeper
#os.environ['ZK_ADDRESS'] = 'zookeeper1:2181,zookeeper2:2181,zookeeper3:2181'
#
##Elasticsearch
#os.environ['ES_CLUSTER'] = 'elasticsearch'
#os.environ['ES_ANALYTICS_INDEX'] = 'analytics'
#os.environ['ES_ANALYTICS_TYPE'] = 'data'
#os.environ['ES_REGISTRY_INDEX'] = 'docker_registry'
#os.environ['ES_REGISTRY_TAG_TYPE'] = 'tags'
#os.environ['ES_REGISTRY_IMAGE_TYPE'] = 'images'

swift_env={
        "OS_TENANT_ID":os.environ["OS_TENANT_ID"],
        "OS_USERNAME": os.environ["OS_USERNAME"],
        "OS_AUTH_URL": os.environ["OS_AUTH_URL"],
        "OS_TENANT_NAME": os.environ["OS_TENANT_NAME"],
        "OS_CONTAINER": os.environ["OS_CONTAINER"],
        "OS_REGION_NAME": os.environ["OS_REGION_NAME"],
        "OS_PASSWORD": os.environ["OS_PASSWORD"],
        "STORAGE_PATH": os.environ["STORAGE_PATH"]
}

es = None
import traceback

def log(data):
    """
    Print debug output

    """
    if (os.environ['DEBUG'] == 'true'):
        print(data + '\n')

def multi_get_from_es_index(index, doc_type, body, _source, fields):
    """
    Get the Elasticsearch index data for multiple ids

    @type  documentList: List
    @param documentList: List of image layer JSON documents
    """
    response = None

    #Try 3 times to read the document from ES, each time picking a random ES node address in case of failure
    for retries in range(3):    
        try:
            response = es.mget(index=index, doc_type=doc_type, body=body, _source=_source, fields=fields)
            #log("ES Get Response :: " + json.dumps(response))
        except ImproperlyConfigured:
            log("ES ImproperlyConfigured!" + traceback.format_exc())
            continue
        except ElasticsearchException:
            log("ES ElasticsearchException!" + traceback.format_exc())
            continue
        except TransportError:
            log("ES TransportError!" + traceback.format_exc())
            continue
        except NotFoundError:
            log("ES NotFoundError!" + traceback.format_exc())
            continue
        except ConflictError:
            log("ES ConflictError!" + traceback.format_exc())
            continue
        except RequestError:
            log("ES RequestError!" + traceback.format_exc())
            continue
        except SerializationError:
            log("ES SerializationError!" + traceback.format_exc())
            continue
        except ConnectionError:
            log("ES ConnectionError!" + traceback.format_exc())
            continue
        except Exception:
            log("ES Exception!" + traceback.format_exc())
            continue
        finally:
            log("Total number of ES read attempts: " + str(retries + 1))
        #Exit for loop if ES transaction is successful otherwise pick another node and continue retrying
        break

    return response
    
def set_in_index(es, document, index, type):
    """
    Store the list of documents in the Elasticsearch index via HTTP APIs

    @type  document: List
    @param document: JSON document
    """
    response = None

    #Try 3 times to store the document in ES, each time picking a random ES node address in case of failure
    for retries in range(3):
        try:
            document['epoch'] = int(time.time())
            log('ES Set Request :: ' + json.dumps(document) + ' : ' + index + ':' + type)
            response = es.index(index=index, doc_type=type, id=document['id'], body=document)
            log("ES Set Response :: " + json.dumps(response))
        except ImproperlyConfigured:
            log("ES ImproperlyConfigured!" + traceback.format_exc())
            continue
        except ElasticsearchException:
            log("ES ElasticsearchException!" + traceback.format_exc())
            continue
        except TransportError:
            log("ES TransportError!" + traceback.format_exc())
            continue
        except NotFoundError:
            log("ES NotFoundError!" + traceback.format_exc())
            continue
        except ConflictError:
            log("ES ConflictError!" + traceback.format_exc())
            continue
        except RequestError:
            log("ES RequestError!" + traceback.format_exc())
            continue
        except SerializationError:
            log("ES SerializationError!" + traceback.format_exc())
            continue
        except ConnectionError:
            log("ES ConnectionError!" + traceback.format_exc())
            continue
        except Exception:
            log("ES Exception!" + traceback.format_exc())
            continue
        finally:
            log("Total number of ES write attempts: " + str(retries + 1))
        #Exit for loop if ES transaction is successful otherwise pick another node and continue retrying
        break

    if response is None or response == '':
        log('Failed to store document ' + document['id'] + ' into the ES index')        
        return 'false'
    else:
        log('Successfully stored document ' + document['id'] + ' into the ES index')
        return 'true'    

def get_es_node_addresses():
    """
    Get the Elasticsearch node addresses via Zookeeper

    @return List of Elasticsearch node ip addresses and ports
    """
    zk = KazooClient(hosts=os.environ['ZK_ADDRESS'], timeout=10.0, randomize_hosts=True)
    zk.start()

    esNodes = []
    try:

        #Fetch the list of ES cluster node names from Zookeeper
        zkPath = '/es/clusters/' + os.environ['ES_CLUSTER'] + '/json'
        children = zk.get_children(zkPath)

        #Retrieve the JSON metadata associated with each ephemeral ES node
        for node in children:
            zookeeperAddr = zkPath + '/' + node
            esNodeInfo = zk.get(zookeeperAddr)
            jsonData = json.loads(esNodeInfo[0])

            #Collect each node ip address and port
            esNodes.append(jsonData['address'] + ':' + jsonData['port'])

    except KazooException:
        log('Kazoo Exception: Unable to fetch Zookeeper data from ' + zkPath + ' : ' + traceback.format_exc());

    zk.stop()
    zk.close()

    log('ES Node list retrieved from Zookeeper :: ' + json.dumps(esNodes))

    return esNodes

def get_image_checksums_from_swift(namespace, repository):
    """
    Get the registry image layer checksums JSON from Swift

    @return Image layer JSON object
    """
    #swift download community-registry registry/images/09690291212c69ac94df172ed35634b5cacd8b52e015e9e81c974cecb8ecde05/json --output -
    swiftCommand = 'swift download ' + swift_env['OS_CONTAINER'] + ' ' + os.environ['SWIFT_REGISTRY_PREFIX'] + '/repositories/' + namespace + '/' + repository + '/_index_images --output -'
    p = subprocess.Popen([swiftCommand], stdout=subprocess.PIPE, shell=True, env=swift_env)
    output, error = p.communicate()
    log('Checksums json from Swift for image ' + namespace + '/' + repository + ' received....' + output)
    return json.loads(output)

def get_image_json_from_swift(checksum):
    """
    Get the registry image layer JSON from Swift

    @return Image layer JSON object
    """
    #swift download community-registry registry/images/09690291212c69ac94df172ed35634b5cacd8b52e015e9e81c974cecb8ecde05/json --output -
    swiftCommand = 'swift download ' + swift_env['OS_CONTAINER'] + ' ' + os.environ['SWIFT_REGISTRY_PREFIX'] + '/images/' + checksum + '/json --output -'
    p = subprocess.Popen([swiftCommand], stdout=subprocess.PIPE, shell=True, env=swift_env)
    output, error = p.communicate()
    log('Image json from Swift for checksum ' + checksum + ' received....')
    return json.loads(output)

def get_tag_checksum_from_swift(namespace, repository, tag):
    """
    Get the registry Tag checksum from Swift

    @return Tag JSON object
    """
    #swift download community-registry registry/repositories/paas/fe15c3d73f634f59904cde910500958b/tag_1408139458.3264 --output -
    swiftCommand = 'swift download ' + swift_env['OS_CONTAINER'] + ' ' + os.environ['SWIFT_REGISTRY_PREFIX'] + '/repositories/' + namespace + '/' + repository + '/tag_' + tag + ' --output -'
    p = subprocess.Popen([swiftCommand], stdout=subprocess.PIPE, shell=True, env=swift_env)
    output, error = p.communicate()
    log('Tag from Swift for ' + namespace + '/' + repository + '/' + tag + ' received....')
    return output

def find_missing_tags_in_es_via_swift(es, days):
    """
    Get the registry Tag names that are present in Swift but absent in the registry ES index

    @return List of Tag names
    """
    #Get the list of tag paths in Swift created N days ago
    tagsList = get_swift_tags(es, days)

    #Now extract the namespace, repository and tag from each path into a JSON document
    docs = generate_tag_docs(tagsList)

    docsNotFound = {}
    if len(docs) > 0:

        #Get all the corresponding Tag ids available in the ES registry index
        #response = es.mget(index=os.environ['ES_REGISTRY_INDEX'], doc_type=os.environ['ES_REGISTRY_TAG_TYPE'], body={"ids" : docs.keys()}, _source=False, fields=[])
        response = multi_get_from_es_index(index=os.environ['ES_REGISTRY_INDEX'], doc_type=os.environ['ES_REGISTRY_TAG_TYPE'], body={"ids" : docs.keys()}, _source=False, fields=[])

        #Iterate over the ES response docs and find the registry tags that haven't been located in the ES index
        #ES sends us a "found=true/false" response attribute per doc so we only need the false ones
        for item in response['docs']:
            if item['found'] == False:
                docsNotFound[item['_id']] = docs.get(item['_id'])

    log(str(len(docsNotFound)) + ' missing tags identified in ES....')
    return docsNotFound

def find_missing_images_in_es_via_swift(es, days):
    """
    Get the registry Image names that are present in Swift but absent in the registry ES index

    @return List of image ids
    """
    #Get the list of tag paths in Swift created N days ago
    imageList = get_swift_images(es, days)

    #Now extract the namespace and repository from each image path into a JSON document
    docs = generate_image_docs(imageList)

    docsNotFound = {}
    if len(docs) > 0:

        #Get all the corresponding image ids available in the ES registry index
        #response = es.mget(index=os.environ['ES_REGISTRY_INDEX'], doc_type=os.environ['ES_REGISTRY_IMAGE_TYPE'], body={"ids" : docs.keys()}, _source=False)
        response = multi_get_from_es_index(index=os.environ['ES_REGISTRY_INDEX'], doc_type=os.environ['ES_REGISTRY_IMAGE_TYPE'], body={"ids" : docs.keys()}, _source=False, fields=[])

        #Iterate over the ES response docs and find the registry images that haven't been located in the ES index
        #ES sends us a "found=true/false" response attribute per doc so we only need the false ones
        for item in response['docs']:
            if item['found'] == False:
                docsNotFound[item['_id']] = docs.get(item['_id'])

    log(str(len(docsNotFound)) + ' missing images identified in ES....')
    return docsNotFound

def generate_tag_docs(tagsList):
    """
    Extract the registry tag names from the list of Swift tag paths. Ex.
    #"paas_c71ffca6470f4f1495d17c729459c8a3_1408369211.39146": {"tag": "1408369211.39146", "namespace": "paas", "repository": "c71ffca6470f4f1495d17c729459c8a3"},
    #"paas_453873689c51492c88341ffea425b3ac_1408322491.23110": {"tag": "1408322491.23110", "namespace": "paas", "repository": "453873689c51492c88341ffea425b3ac"},
    #........

    @return List of JSON documents with the namespace, repository and tag info
    """
    docs = {}
    for item in tagsList:
        if (item != ''):
            temp = item.split('/')

            #Generate a document for the missing Tag
            doc = {}
            doc['namespace'] = temp[2]
            doc['repository'] = temp[3]
            doc['tag'] = temp[4].replace('tag_', '')

            #Add the missing tag info to the ids list and docs dict resp.
            docs[temp[2] + '_' + temp[3] + '_' + doc['tag']] = doc

    log('Documents generated for ' + str(len(docs)) + ' tags....')
    return docs

def generate_image_docs(imageList):
    """
    Extract the registry image docs from the list of Swift image paths. Ex.
    #"paas_c71ffca6470f4f1495d17c729459c8a3": {"namespace": "paas", "repository": "c71ffca6470f4f1495d17c729459c8a3"},
    #"paas_453873689c51492c88341ffea425b3ac": {"namespace": "paas", "repository": "453873689c51492c88341ffea425b3ac"},
    #........

    @return List of JSON documents with the namespace, repository and tag info
    """
    docs = {}
    for item in imageList:
        if (item != ''):
            temp = item.split('/')

            #Generate a document for the missing Tag
            doc = {}
            doc['namespace'] = temp[2]
            doc['repository'] = temp[3]

            #Add the missing tag info to the ids list and docs dict resp.
            docs[temp[2] + '_' + temp[3]] = doc

    log('Documents generated for ' + str(len(docs)) + ' images....')
    return docs

def get_swift_tags(es, days):
    """
    Get all the registry Tags created in Swift during the past N days
    #registry/repositories/paas/01a66295ebd74d9199817940531c1d46/tag_1408320061.71133
    #........
    #........

    @return List of Swift Tag location paths
    """
    #Generate the date to grep for in yyyy-mm-dd format in the Swift output
    dateString=str(date.today()-timedelta(days=days))

    #Get the list of tag paths from Swift via the Swift cli
    swiftCommand = 'swift list ' + swift_env['OS_CONTAINER'] + ' --prefix "' + os.environ['SWIFT_REGISTRY_PREFIX'] + '/repositories" --long | grep "tag_" | grep "' + dateString + '" |  sed "s#.* ##g"'
    p = subprocess.Popen([swiftCommand], stdout=subprocess.PIPE, shell=True, env=swift_env)
    output, error = p.communicate()

    #Convert the Tag list string into an array
    tagsList = output.split('\n')
    tagsList = filter(None, tagsList)

    log(str(len(tagsList)) + ' tags created in Swift ' + str(days) + ' days ago on that date ' + dateString + '....')
    return tagsList

def get_swift_images(es, days):
    """
    Get all the registry images created in Swift during the past N days. Ex.
    # registry/repositories/paas/0782049940714c6f9269b2879073d707/_index_images
    # registry/repositories/paas/10bad43969474fec80ba5465bec62412/_index_images
    # ........
    # ........

    @return List of Swift image location paths
    """
    #Generate the date to grep for in yyyy-mm-dd format in the Swift output
    dateString=str(date.today()-timedelta(days=days))

    #Get the list of tag paths from Swift via the Swift cli
    swiftCommand = 'swift list ' + swift_env['OS_CONTAINER'] + ' --prefix "' + os.environ['SWIFT_REGISTRY_PREFIX'] + '/repositories" --long | grep "_index_images" | grep "' + dateString + '" |  sed "s#.* ##g"'
    p = subprocess.Popen([swiftCommand], stdout=subprocess.PIPE, shell=True, env=swift_env)
    output, error = p.communicate()

    #Convert the image list string into an array
    imageList = output.split('\n')
    imageList = filter(None, imageList)

    log(str(len(imageList)) + ' images/repos created in Swift ' + str(days) + ' days ago....')
    return imageList

def generate_tag_document(namespace, repository, tag, checksum, author):
    """
    Creates a Tag in the registry index

    """
    #Manufacture the tag document to be writen into the index
    document={}

    #Concatenate the <namespace>_<repository>_imageid to generate a unique primary key id
    document['id'] = namespace + '_' + repository + '_' + tag
    document['namespace'] = namespace
    document['tag'] = tag
    document['checksum'] = checksum
    document['repository'] = repository
    document['description'] = author

    log('Generated ES document for Tag ' + document['id'] + '....')
    return document

def generate_image_document(namespace, repository, checksums):
    """
    Creates an Image document in the registry index

    """
    #Manufacture the image document to be writen into the index
    document={}

    #Concatenate the <namespace>_<repository> to generate a unique primary key id
    document['id'] = namespace + '_' + repository
    document['namespace'] = namespace
    document['imageLayers'] = checksums
    document['repository'] = repository

    log('Generated ES document for Image ' + document['id'] + '....')
    return document

#Overriding the default ES Sniffing mechanism with Zookeeper
class ZookeeperTransport(Transport):

    def get_es_node_addresses(self):
        """
        Get the Elasticsearch node addresses via Zookeeper

        @return List of Elasticsearch node ip addresses and ports
        """
        esNodes = []

        #Initlate the Zookeeper Kazoo connection
        #kz_retry = KazooRetry(max_tries=3, delay=0.5, backoff=2)
        zk = KazooClient(hosts=os.environ['ZK_ADDRESS'], timeout=10.0, randomize_hosts=True)
        zk.start()

        try:

            #Fetch the list of ES cluster node names from Zookeeper
            zkPath = '/es/clusters/' + os.environ['ES_CLUSTER'] + '/json'
            children = zk.get_children(zkPath)

            #Retrieve the JSON metadata associated with each ephemeral ES node
            for node in children:
                zookeeperAddr = zkPath + '/' + node
                esNodeInfo = zk.get(zookeeperAddr)
                jsonData = json.loads(esNodeInfo[0])

                #Collect each node ip address and port
                host = {'host':jsonData['address'], 'port': int(jsonData['port'])}
                esNodes.append(host)

        except KazooException:
            log('Kazoo Exception: Unable to fetch Zookeeper data from ' + zkPath + ' : ' + traceback.format_exc());

        #Close and Zookeeper connection
        zk.stop()
        zk.close()

        return esNodes

    def sniff_hosts(self):
        """
        Obtain a list of nodes from the cluster and create a new connection
        pool using the information retrieved.

        To extract the node connection parameters use the `nodes_to_host_callback`.
        """
        previous_sniff = self.last_sniff
        hosts = []
        try:
            # reset last_sniff timestamp
            self.last_sniff = time.time()
            try:
                hosts = self.get_es_node_addresses()
            except Exception:
                raise TransportError("N/A", "Unable to sniff hosts." + traceback.format_exc())
        except:
            # keep the previous value on error
            self.last_sniff = previous_sniff
            raise

        # we weren't able to get any nodes, maybe using an incompatible
        # transport_schema or host_info_callback blocked all - raise error.
        if not hosts:
            raise TransportError("N/A", "Unable to sniff hosts - no viable hosts found." + traceback.format_exc())

        self.set_connections(hosts)

if __name__ == "__main__":

    #Initiate the ES connection pool
    es = Elasticsearch(get_es_node_addresses(), sniff_on_start=True, sniff_on_connection_fail=True, max_retries=3, sniffer_timeout=180, selector_class=RoundRobinSelector, sniff_timeout=1, transport_class=ZookeeperTransport)

    while True:

        #Find missing tags
        missingTagDocs = find_missing_tags_in_es_via_swift(es, int(os.environ['DAYS']))

        #Iterate over the missing tag docs and restore them in the ES index
        counter = 1
        total = len(missingTagDocs.items())
        for key, value in missingTagDocs.items():
            log('**** Restoring Tag ' + str(counter) + '/' + str(total) + ' --> ' + value['namespace'] + '/' + value['repository'] + '/' + value['tag'] + ' ****\n')
            checksum = get_tag_checksum_from_swift(value['namespace'], value['repository'], value['tag'])
            data = get_image_json_from_swift(checksum)
            document = generate_tag_document(value['namespace'], value['repository'], value['tag'], data.get('id'), data.get('author', ''))
            set_in_index(es, document, os.environ['ES_REGISTRY_INDEX'], os.environ['ES_REGISTRY_TAG_TYPE'])
            counter = counter + 1
            log('************************************************************************************************************\n')

        #Find missing images from Swift
        missingImageDocs = find_missing_images_in_es_via_swift(es, int(os.environ['DAYS']))

        #Iterate over the missing image docs and restore them in the ES index
        counter = 1
        total = len(missingImageDocs.items())
        for key, value in missingImageDocs.items():
            log('**** Restoring Image' + str(counter) + '/' + str(total) + ' --> ' + value['namespace'] + '/' + value['repository'] + ' ****\n')
            checksumDict = get_image_checksums_from_swift(value['namespace'], value['repository'])
            checksumList = []
            for checksum in checksumDict:
                checksumList.append(checksum['id'])
            document = generate_image_document(value['namespace'], value['repository'], checksumList)
            set_in_index(es, document, os.environ['ES_REGISTRY_INDEX'], os.environ['ES_REGISTRY_IMAGE_TYPE'])
            counter = counter + 1
            log('**********************************************************************************************\n')

        log('Sleeping for ' + os.environ['CRON'] + ' secs.....\n')
        time.sleep(int(os.environ['CRON']))
