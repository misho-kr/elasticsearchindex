import time
import simplejson as json
from elasticsearch.transport import Transport, TransportError
from kazoo.client import KazooClient
#from kazoo.retry import KazooRetry
from kazoo.exceptions import (KazooException)
import logging
from docker_registry.lib import config
logger = logging.getLogger(__name__)
cfg = config.load()
import traceback

class ZookeeperTransport(Transport):

    def get_es_node_addresses(self):
        """
        Get the Elasticsearch node addresses via Zookeeper

        @return List of Elasticsearch node ip addresses and ports
        """
        esNodes = []

        #Initlate the Zookeeper Kazoo connection
        #kz_retry = KazooRetry(max_tries=3, delay=0.5, backoff=2)
        zk = KazooClient(hosts=cfg.search_options.zk_address, timeout=10.0, randomize_hosts=True)
        zk.start()

        try:

            #Fetch the list of ES cluster node names from Zookeeper
            zkPath = '/es/clusters/' + cfg.search_options.es_cluster + '/json'
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
            logger.Error('Kazoo Exception: Unable to fetch Zookeeper data from ' + zkPath + ' : ' + traceback.format_exc());

        #Terminate the Zookeeper connection
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
