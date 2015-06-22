#! /usr/bin/python

__author__="Ashish Hunnargikar"
__date__ ="$Jun 13, 2014 12:33:33 PM$"

import time
import os
from elasticsearch.transport import Transport
from elasticsearch import (Elasticsearch, RoundRobinSelector, ImproperlyConfigured, ElasticsearchException, 
    SerializationError, TransportError, NotFoundError, ConflictError, RequestError, ConnectionError)
import simplejson as json
from kazoo.client import KazooClient
from kazoo.exceptions import (KazooException)
os.environ['DEBUG'] = 'true'

##Zookeeper
#os.environ['ZK_ADDRESS'] = 'zookeeper1:2181,zookeeper2:2181,zookeeper3:2181'
#
##Elasticsearch
#os.environ['ES_CLUSTER'] = 'elasticsearch'
#os.environ['ES_ANALYTICS_INDEX'] = 'analytics'
#os.environ['ES_ANALYTICS_TYPE'] = 'data'
#os.environ['ES_REGISTRY_INDEX'] = 'docker_registry'
#os.environ['ES_REGISTRY_TYPE'] = 'tags'

es = None
import traceback

def log(data):
    """
    Print debug output

    """
    if (os.environ['DEBUG'] == 'true'):
        print(data + '\n')

def get_from_index(index, type, id):
    """
    Get the Elasticsearch index data

    @type  documentList: List
    @param documentList: List of image layer JSON documents
    """    
    response = None
    
    #Try 3 times to read the document from ES, each time picking a random ES node address in case of failure
    for retries in range(3):    
        try:
            response = es.get(index=index, doc_type=type, id=id)
            log("ES Get Response :: " + json.dumps(response))
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

    if response is None or response == '':
        return ('false', retries + 1)
    else:
        return ('true', retries + 1)
    
def set_in_index(document, index, type):
    """
    Store the list of documents in the Elasticsearch index via HTTP APIs

    @type  document: List
    @param document: JSON document
    """
    response = None

    #Try 3 times to store the document in ES, each time picking a random ES node address in case of failure
    for retries in range(3):
        try:
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
        return 'false'
    else:
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

def calculate_build_time_percentiles(es):

    response = es.search(index=os.environ['ES_ANALYTICS_INDEX'], doc_type=os.environ['ES_ANALYTICS_TYPE'], body=
        {
        "query": {
                "bool": {
                    "must": [
                                        {"match": {"build": "true"}}
                    ]
                }
            },
            "size":0,
            "aggs" : {
                "load_time_outlier" : {
                    "percentiles" : {
                        "field" : "build-time"
                    }
                }
            }
        })

    print_percentiles(response)

def calculate_push_time_percentiles(es):

    response = es.search(index=os.environ['ES_ANALYTICS_INDEX'], doc_type=os.environ['ES_ANALYTICS_TYPE'], body=
        {
        "query": {
                "bool": {
                    "must": [
                                        {"match": {"build": "true"}},
                                        {"match": {"push": "true"}}
                    ]
                }
            },
            "size":0,
            "aggs" : {
                "load_time_outlier" : {
                    "percentiles" : {
                        "field" : "push-time"
                    }
                }
            }
        })

    print_percentiles(response)

def calculate_pull_time_percentiles(es):

    response = es.search(index=os.environ['ES_ANALYTICS_INDEX'], doc_type=os.environ['ES_ANALYTICS_TYPE'], body=
        {
        "query": {
                "bool": {
                    "must": [
                                        {"match": {"build": "true"}},
                                        {"match": {"push": "true"}},
                                        {"match": {"pull": "true"}}
                    ]
                }
            },
            "size":0,
            "aggs" : {
                "load_time_outlier" : {
                    "percentiles" : {
                        "field" : "pull-time"
                    }
                }
            }
        })

    print_percentiles(response)

def calculate_get_time_percentiles(es):

    response = es.search(index=os.environ['ES_ANALYTICS_INDEX'], doc_type=os.environ['ES_ANALYTICS_TYPE'], body=
        {
        "query": {
                "bool": {
                    "must": [
                                        {"match": {"build": "true"}},
                                        {"match": {"push": "true"}},
                                        {"match": {"get": "true"}}
                    ]
                }
            },
            "size":0,
            "aggs" : {
                "load_time_outlier" : {
                    "percentiles" : {
                        "field" : "get-time"
                    }
                }
            }
        })

    print_percentiles(response)

def print_percentiles(response):

    #Sort the percentile data
    percentileKeys = response['aggregations']['load_time_outlier']['values'].keys()
    percentileKeys = map(float, percentileKeys)
    percentileKeys.sort()
    
    log('Percentiles in Secs:')
    for percentileKey in percentileKeys:
        log(str(percentileKey) + "% : " + str(response['aggregations']['load_time_outlier']['values'][str(percentileKey)]))

def calculate_stats(successCondition, failureCondition):

    successResponse = es.search(index=os.environ['ES_ANALYTICS_INDEX'], doc_type=os.environ['ES_ANALYTICS_TYPE'], body=
        {
        "query": {
                "bool": {
                    "must": successCondition
                }
            },
            "size":0
        })

    failureResponse = es.search(index=os.environ['ES_ANALYTICS_INDEX'], doc_type=os.environ['ES_ANALYTICS_TYPE'], body=
        {
        "query": {
                "bool": {
                    "must": failureCondition
                }
            },
            "size":0
        })

    success = successResponse['hits']['total']
    failure = failureResponse['hits']['total']
    successPercent = (float(success)/float((failure + success)))*100
    failurePercent = (float(failure)/float((failure + success)))*100
    log('Totals: ' + str(success) + '/' + str(failure+success))
    log('Success: ' + str(successPercent) + '%')
    log('Failure: ' + str(failurePercent) + '%')

if __name__ == "__main__":

    #Initiate the ES connection pool
    es = Elasticsearch(get_es_node_addresses(), sniff_on_start=True, sniff_on_connection_fail=True, max_retries=3, sniffer_timeout=180, selector_class=RoundRobinSelector, sniff_timeout=1, transport_class=ZookeeperTransport)

    log('############## DOCKER BUILD STATS ####################')
    calculate_stats([{"match": {"build": "true"}}], [{"match": {"build": "false"}}])
    calculate_build_time_percentiles(es)

    log('############## DOCKER PUSH STATS ####################')
    calculate_stats([{"match": {"build": "true"}},{"match": {"push": "true"}}], [{"match": {"build": "true"}}, {"match": {"push": "false"}}])
    calculate_push_time_percentiles(es)

    log('############## DOCKER PULL STATS ####################')
    calculate_stats([{"match": {"build": "true"}},{"match": {"push": "true"}}, {"match": {"pull": "true"}} ], [{"match": {"build": "true"}}, {"match": {"push": "true"}}, {"match": {"pull": "false"}}])
    calculate_pull_time_percentiles(es)    

    log('############## ES GET STATS ####################')
    calculate_stats([{"match": {"build": "true"}},{"match": {"push": "true"}}, {"match": {"get": "true"}} ], [{"match": {"build": "true"}}, {"match": {"push": "true"}}, {"match": {"get": "false"}}])
    calculate_get_time_percentiles(es)
