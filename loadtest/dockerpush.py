#! /usr/bin/python

__author__="Ashish Hunnargikar"
__date__ ="$Jun 13, 2014 12:33:33 PM$"

import time
import random
import subprocess
import os
import uuid
import datetime
from elasticsearch.transport import Transport
from elasticsearch import (Elasticsearch, RoundRobinSelector, ImproperlyConfigured, ElasticsearchException, 
    SerializationError, TransportError, NotFoundError, ConflictError, RequestError, ConnectionError)
import simplejson as json
from kazoo.client import KazooClient
from kazoo.exceptions import (
    KazooException
)
os.environ['DEBUG'] = 'true'
#os.environ['REGISTRY_ADDR'] = '192.168.57.101:5000'
#os.environ['ES_CLUSTER'] = 'elasticsearch'
#os.environ['ZK_ADDRESS'] = 'zookeeper1:2181,zookeeper2:2181,zookeeper3:2181'
#os.environ['ES_INDEX'] = 'docker_registry'
#os.environ['LIBPROCESS_IP'] = '192.168.57.101'
#os.environ['SIZE_RANGE_START'] = '2'
#os.environ['SIZE_RANGE_END'] = '3'
es = None
import traceback

def log(data):
    """
    Print debug output

    """
    if (os.environ['DEBUG'] == 'true'):
        print(data + '\n')

def initiate_load_test(repository):
    """
    Build a Busybox Dockerfile with dynamic layers and push the image to the Docker registry

    """
    #1. Generate a random unique tag
    timestamp = str(time.time())
    sizeRange = str(random.randint(int(os.environ['SIZE_RANGE_START']), int(os.environ['SIZE_RANGE_END'])))
    tag = timestamp + sizeRange
    document = {}
    document['id'] = 'paas_' + repository + '_' + tag
    document['size'] = sizeRange
    document['start-time'] = timestamp
    document['command'] = 'docker pull ' + os.environ['REGISTRY_ADDR']  + '/paas/' + repository + ':' + tag

    #2. Create a randomly-sized file layer
    create_file(repository, sizeRange)

    #3. Create a new Dockerfile with random metadata
    create_dockerfile(repository, tag)

    #4. Build the Docker image
    start = datetime.datetime.now()
    document['build'] = build(repository, tag)
    end = datetime.datetime.now()
    document['build-time'] = (end - start).total_seconds()

    #5. Push the Docker image
    start = datetime.datetime.now()

    #Try 3 times to push the docker image in the event of failure
    for pushes in range(3):
        try:
            document['push'] = push(repository, tag)
            if (document['push'] == 'false'):
                continue
        except Exception:
            log("Push Exception!" + traceback.format_exc())
            continue
        finally:
            log("Total number of Docker push attempts: " + str(pushes + 1))
        break

    end = datetime.datetime.now()
    document['push-time'] = (end - start).total_seconds()
    document['push-tries'] = pushes + 1

    #6. Remove the Docker image
    start = datetime.datetime.now()
    document['rmi'] = rmi(repository, tag)
    end = datetime.datetime.now()
    document['rmi-time'] = (end - start).total_seconds()

    #7. Pull the Docker image
    start = datetime.datetime.now()
    document['pull'] = pull(repository, tag)
    end = datetime.datetime.now()
    document['pull-time'] = (end - start).total_seconds()

    #8. Run the Docker image
    start = datetime.datetime.now()
    document['run'] = run(repository, tag, sizeRange)
    end = datetime.datetime.now()
    document['run-time'] = (end - start).total_seconds()

    #9. Clean up the generated Docker image and associated containers
    cleanup(repository, tag)

    #10. Verify that the index contains the image layers
    start = datetime.datetime.now()
    document['get'], document['get-tries'] = get_from_index(os.environ['ES_INDEX'], 'tags', document['id'])
    end = datetime.datetime.now()
    document['get-time'] = (end - start).total_seconds()

    #11. Analytics
    document['end-time'] = str(time.time())
    document['host'] = os.environ['LIBPROCESS_IP']
    log(json.dumps(document))
    set_in_index(document, 'analytics', 'data')

def create_file(repository, range):
    """
    Creates randomly sized file on disk using the system entropy

    @type  repository: String
    @param repository: Docker image repository name
    @type  range: Integer
    @param range: File size
    """
    log('App Name : ' + repository)
    log('File size : ' + range + 'MB')
    command = 'dd if=/dev/urandom of=file.txt bs=' + range + 'M count=1'
    subprocess.call(command, shell=True)

def create_dockerfile(repository, tag):
    """
    Creates a Dockerfile and includes an ADD instruction for the randomly generated file

    @type  repository: String
    @param repository: Docker image repository name
    @type  tag: String
    @param tag: Docker repository tag name
    """
    metadata = get_random_metadata()
    Dockerfile = open('Dockerfile', 'w')
    Dockerfile.write('FROM busybox\n')
    Dockerfile.write('MAINTAINER ' + metadata + '\n')
    Dockerfile.write('ADD file.txt file.txt\n')
    Dockerfile.close()

def build(repository, tag):
    """
    Builds a new Docker image using a pre-generated Dockerfile using the Docker cli client

    @type  repository: String
    @param repository: Docker image repository name
    @type  tag: String
    @param tag: Docker repository tag name
    @return:  True if successful otherwise false
    """
    p = subprocess.Popen(['docker build --rm -t ' + os.environ['REGISTRY_ADDR'] + '/paas/' + repository + ':' + tag + ' .'], stdout=subprocess.PIPE, shell=True)
    output, error = p.communicate()
    log('BUILD OUTPUT :: ' + json.dumps(output))
    if error is None and 'Successfully built' in output:
        return 'true'
    else:
        log('BUILD OUTPUT :: ' + json.dumps(output))        
        log('*********BUILD ERROR :: ' + json.dumps(error))
        return 'false'

def push(repository, tag):
    """
    Pushes the pre-built Docker image into the Docker registry via the Docker cli client

    @type  repository: String
    @param repository: Docker image repository name
    @type  tag: String
    @param tag: Docker repository tag name
    @return:  True if successful otherwise false
    """
    log('docker push ' + os.environ['REGISTRY_ADDR'] + '/paas/' + repository + ':' + tag)
    p = subprocess.Popen(['docker push ' + os.environ['REGISTRY_ADDR'] + '/paas/' + repository + ':' + tag], stdout=subprocess.PIPE, shell=True)
    output, error = p.communicate()
    log('PUSH OUTPUT :: ' + json.dumps(output))
    if error is None and 'Pushing tag for rev' in output:
        return 'true'
    else:
        log('PUSH OUTPUT :: ' + json.dumps(output))
        log('*********PUSH ERROR :: ' + json.dumps(error))
        return 'false'

def rmi(repository, tag):
    """
    Removes a pre-built Docker image via the Docker cli client

    @type  repository: String
    @param repository: Docker image repository name
    @type  tag: String
    @param tag: Docker repository tag name
    @return:  True if successful otherwise false
    """
    p = subprocess.Popen(['docker rmi -f ' + os.environ['REGISTRY_ADDR'] + '/paas/' + repository + ':' + tag], stdout=subprocess.PIPE, shell=True)
    output, error = p.communicate()
    log('RMI OUTPUT :: ' + json.dumps(output))
    if error is None and 'Untagged' in output:
        return 'true'
    else:
        log('RMI OUTPUT :: ' + json.dumps(output))
        log('*********RMI ERROR :: ' + json.dumps(error))
        return 'false'

def pull(repository, tag):
    """
    Pulls a new Docker image and tag from the Docker registry via the Docker cli client

    @type  repository: String
    @param repository: Docker image repository name
    @type  tag: String
    @param tag: Docker repository tag name
    @return:  True if successful otherwise false    
    """
    p = subprocess.Popen(['docker pull ' + os.environ['REGISTRY_ADDR'] + '/paas/' + repository + ':' + tag], stdout=subprocess.PIPE, shell=True)
    output, error = p.communicate()
    log('PULL OUTPUT :: ' + json.dumps(output))
    if error is None and output is not None and 'Pulling repository' in output:
        return 'true'
    else:
        log('PULL OUTPUT :: ' + json.dumps(output))
        log('*********PULL ERROR :: ' + json.dumps(error))
        return 'false'

def run(repository, tag, range):
    """
    Runs a Docker image via the Docker cli client

    @type  repository: String
    @param repository: Docker image repository name
    @type  tag: String
    @param tag: Docker repository tag name
    @return:  True if successful otherwise false
    """
    p = subprocess.Popen(['docker run -t ' + os.environ['REGISTRY_ADDR'] + '/paas/' + repository + ':' + tag + ' du -ms file.txt | cut -f1'], stdout=subprocess.PIPE, shell=True)
    output, error = p.communicate()
    log('RUN OUTPUT :: ' + json.dumps(output))
    if error is None and output is not None and output != '' and int(range) == int(output.replace('\n', '')):
        return 'true'
    else:
        log('RUN OUTPUT :: ' + json.dumps(output))
        log('**********RUN ERROR :: ' + json.dumps(error))
        return 'false'

def cleanup(repository, tag):
    """
    Cleans up the Docker image via the Docker cli client

    @type  repository: String
    @param repository: Docker image repository name
    @type  tag: String
    @param tag: Docker repository tag name
    """
    p = os.popen('docker rm `docker ps -a -q`')
    rmi(repository, tag)

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
        return 'false'
    else:
        return 'true'

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

def get_random_metadata():
    """
    Generate random metadata blurb via Wikipedia

    @return String containing a small paragraph of a random Wikipedia article
    """
    p = os.popen('curl -L http://en.wikipedia.org/wiki/Special:Random | grep -o \'<p>.*</p>\' | sed -e \'s/<[^>]*>//g\'')
    s = p.readline().replace('\n', '')
    p.close()
    log('######################################')
    log('RANDOM METADATA :: ' + s)
    log('######################################')
    return s

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

    #Generate a unique namespace
    repository=str(uuid.uuid4()).replace('-', '');

    #initiate_load_test(repository)
   #Run the Docker build-push-pull-run operation in an infinite loop
    while True:
        initiate_load_test(repository)
        sleepInterval = random.randint(2,10)
        log('Sleeping for ' + str(sleepInterval) + ' secs.....\n')
        log('************************************\n')
        time.sleep(sleepInterval)
