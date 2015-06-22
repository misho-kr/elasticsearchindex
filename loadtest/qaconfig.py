#! /usr/bin/python

__author__="Ashish Hunnargikar"
__date__ ="$Jun 13, 2014 12:33:33 PM$"

import os
import time

#########################
##COMMON CONFIGS
#########################
randomString = os.urandom(16).encode('hex') + str(time.time())
marathonUrl = 'http://<marathon-url>:8080/v2/apps/'
zookeeperAddr = '<zk-host1>:2181,<zk-host2>:2181,<zk-host3>:2181,<zk-host1>2181,<zk-host1>:2181'
es_index = 'docker_registry'

#########################
##ELASTICSEARCH CONFIG
#########################
es_clusterName = 'elasticsearch-' + randomString
#es_command = 'env; wget http://<github-repo>/ahunnargikar/elasticsearch-1.2.0/archive/master.zip; unzip master.zip; cd elasticsearch-1.2.0-master; export ES_HEAP_SIZE=8g; export ES_MIN_MEM=8g; export ES_MAX_MEM=8g; (echo "discovery:"; echo " type: com.sonian.elasticsearch.zookeeper.discovery.ZooKeeperDiscoveryModule"; echo "sonian.elasticsearch.zookeeper:"; echo " settings.enabled: true"; echo " client.host: ' + zookeeperAddr + '"; echo " discovery.state_publishing.enabled: true") >> config/elasticsearch.yml; bin/elasticsearch -Des.default.config=config/elasticsearch.yml -Des.default.path.home=$(pwd) -Des.default.path.logs=logs/ -Des.default.path.data=data/ -Des.default.path.work=/tmp -Des.default.path.conf=config/ -Des.network.host=$LIBPROCESS_IP -Des.http.port=$PORT0 -Des.transport.tcp.port=$PORT1 -Des.cluster.name=' + es_clusterName + ' -Des.node.name=$MESOS_EXECUTOR_ID -Des.index.number_of_replicas="2" -Des.discovery.zen.ping.multicast.enabled=false;'
es_command = 'env; export ES_HEAP_SIZE=8g; wget https://<github-repo>/ahunnargikar/ebay-elasticsearch-1.2.0/archive/master.zip; unzip master.zip; cd ebay-elasticsearch-1.2.0-master; (echo "discovery:"; echo " type: com.sonian.elasticsearch.zookeeper.discovery.ZooKeeperDiscoveryModule"; echo "sonian.elasticsearch.zookeeper:"; echo " settings.enabled: true"; echo " client.host: ' + zookeeperAddr + '"; echo " discovery.state_publishing.enabled: true") >> config/elasticsearch.yml; bin/elasticsearch -Des.default.config=config/elasticsearch.yml -Des.default.path.home=$(pwd) -Des.default.path.logs=logs/ -Des.default.path.data=data/ -Des.default.path.work=/tmp -Des.default.path.conf=config/ -Des.network.host=$LIBPROCESS_IP -Des.http.port=$PORT0 -Des.transport.tcp.port=$PORT1 -Des.cluster.name=' + es_clusterName + ' -Des.node.name=$MESOS_EXECUTOR_ID -Des.index.number_of_replicas="2" -Des.discovery.zen.ping.multicast.enabled=false;'
es_constraints = [["general","LIKE","true"]]
es_cpus = 1
es_env = {}
es_executor = ''
es_instances =12
es_memory = 8192
es_ports = [0,0]
es_uris = []

#########################
##DOCKER REGISTRY CONFIG
#########################
dr_clusterName = 'registry-' + randomString
dr_command = ''
dr_constraints = [["community", "LIKE", "registry"]]
dr_container = {"image":"docker:///<docker-registry>/hashish/docker-registry"}
dr_cpus = 1
dr_env = {"SETTINGS_FLAVOR": "swift-es", "STORAGE_PATH":"/registry", "OS_AUTH_URL":"https://<os-identity>:5443/v2.0", "OS_CONTAINER":"community-registry", "OS_USERNAME":"<os-user>", "OS_PASSWORD":"<os-password>", "OS_TENANT_NAME":"<os-tenant>", "OS_REGION_NAME":"<os-region>"}
dr_executor = ''
dr_instances = 1
dr_memory = 4096
dr_ports = [80,443, 5000]
dr_uris = []

#########################
##DOCKER BUILD & PUSH CONFIG
#########################
dbp_clusterName = 'docker-' + randomString
#dbp_command = 'env; wget -O dockerpush.py https://<github-repo>/ahunnargikar/elasticsearchindex/raw/master/loadtest/dockerpush.py; chmod +x dockerpush.py; python dockerpush.py;'
dbp_command = 'env; wget -O dockerpush.py https://<github-repo>/ahunnargikar/github_elasticsearchindex/raw/master/loadtest/dockerpush.py; chmod +x dockerpush.py; python dockerpush.py;'
#dbp_constraints = [["clusterType", "LIKE", "elasticsearch1"]]
dbp_constraints = []
dbp_cpus = 0.1
dbp_env = {'SIZE_RANGE_START': 50, 'SIZE_RANGE_END': 150}
dbp_executor = ''
dbp_instances = 1 #Number of load test jobs to be launched per registry
dbp_memory = 64
dbp_ports = []
dbp_uris = []

#########################
##HAPROXY CONFIG
#########################
ha_clusterName = 'haproxy-' + randomString
ha_command = 'export LIBPROCESS_IP=$(/sbin/ifconfig eth0 | grep \'inet addr:\' | cut -d: -f2 | awk \'{ print $1}\'); env; wget http://<github-repo>/ahunnargikar/haproxy-mesos/archive/master.zip; unzip master.zip; cd haproxy-mesos-master; python haproxy.py $REGISTRY_ADDR; sed -i "s#5001#$PORT0#g" haproxy.cfg; chmod +x haproxy; ./haproxy -db -f haproxy.cfg;'
ha_constraints = []
ha_cpus = 1
ha_env = {}
ha_executor = ''
ha_instances = 1
ha_memory = 64
ha_ports = [0]
ha_uris = []