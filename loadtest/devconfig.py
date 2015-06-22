#! /usr/bin/python

__author__="Ashish Hunnargikar"
__date__ ="$Jun 13, 2014 12:33:33 PM$"

import os
import time

#########################
##COMMON CONFIGS
#########################
randomString = os.urandom(16).encode('hex') + str(time.time())
marathonUrl = 'http://marathon1/v2/apps/'
zookeeperAddr = '192.168.57.101:2181,192.168.57.102:2181,192.168.57.103:2181'
es_index = 'docker_registry'

#########################
##ELASTICSEARCH CONFIG
#########################
es_clusterName = 'elasticsearch-' + randomString
es_command = 'wget https://<github-repo>/ahunnargikar/ebay-elasticsearch-1.2.0/archive/master.zip; unzip master.zip; cd ebay-elasticsearch-1.2.0-master; cp config/elasticsearch.yml.dev config/elasticsearch.yml;  (echo "discovery:"; echo " type: com.sonian.elasticsearch.zookeeper.discovery.ZooKeeperDiscoveryModule"; echo "sonian.elasticsearch.zookeeper:"; echo " settings.enabled: true"; echo " client.host: ' + zookeeperAddr + '"; echo " discovery.state_publishing.enabled: true") >> config/elasticsearch.yml; bin/elasticsearch -Des.default.config=config/elasticsearch.yml -Des.default.path.home=$(pwd) -Des.default.path.logs=logs/ -Des.default.path.data=data/ -Des.default.path.work=/tmp -Des.default.path.conf=config/ -Des.network.host=$LIBPROCESS_IP -Des.http.port=$PORT0 -Des.transport.tcp.port=$PORT1 -Des.cluster.name=' + es_clusterName + ' -Des.node.name=$MESOS_EXECUTOR_ID -Des.index.number_of_replicas="2" -Des.discovery.zen.ping.multicast.enabled=false;'
es_constraints = [["clusterType", "LIKE", "elasticsearch2"]]
#es_constraints = []
es_cpus = 1
es_env = {}
es_executor = ''
es_instances =3
es_memory = 128
es_ports = [0,0]
es_uris = []

#########################
##DOCKER REGISTRY CONFIG
#########################
dr_clusterName = 'registry-' + randomString
dr_command = ''
dr_constraints = [["clusterType", "LIKE", "elasticsearch1"]]
#dr_container = {"image":"docker:///<docker-registry>/mkrastev/docker-registry:devtest2"}
dr_container = {"image":"docker:///<docker-registry>/hashish/docker-registry"}
dr_cpus = 1
dr_env = {"SETTINGS_FLAVOR": "swift-es", "STORAGE_PATH":"/registry", "OS_AUTH_URL":"https://<os-identity>:5443/v2.0", "OS_CONTAINER":"ahunnargikar", "OS_USERNAME":"<os-user>", "OS_PASSWORD":"<os-password>", "OS_TENANT_NAME":"<os-tenant>", "OS_REGION_NAME":"<os-region>"}
dr_executor = ''
dr_instances = 1
dr_memory = 256
dr_ports = [80,443, 5000]
dr_uris = []

#dr_command = "env; wget http://<github-repo>/ahunnargikar/docker-registry/archive/elasticsearch-backend-fix.zip; unzip elasticsearch-backend-fix.zip; cd docker-registry-elasticsearch-backend-fix; mv config/config.swift.yml config/config.yml; export REGISTRY_HOME=$(pwd); export DOCKER_REGISTRY_CONFIG=$(pwd)/config/config.yml; export SETTINGS_FLAVOR=dev; exec gunicorn -k gevent --max-requests 100 --graceful-timeout 3600 -t 3600 -b $LIBPROCESS_IP:$PORT0 -w 8 --access-logfile ../access.log --error-logfile ../server.log docker_registry.wsgi:application;"
#dr_command = "env; wget http://<github-repo>/ahunnargikar/docker-registry/archive/elasticsearch-backend-fix.zip; unzip elasticsearch-backend-fix.zip; cd docker-registry-elasticsearch-backend-fix; export REGISTRY_HOME=$(pwd); export DOCKER_REGISTRY_CONFIG=$(pwd)/config/config.yml; export SETTINGS_FLAVOR=dev; exec gunicorn -k gevent --max-requests 100 --graceful-timeout 3600 -t 3600 -b $LIBPROCESS_IP:$PORT0 -w 8 --access-logfile ../access.log --error-logfile ../server.log docker_registry.wsgi:application;"
#dr_constraints = [["clusterType", "LIKE", "elasticsearch2"]]
#dr_cpus = 0.1
#dr_env = {}

#########################
##DOCKER BUILD & PUSH CONFIG
#########################
dbp_clusterName = 'loadtest-client-' + randomString
#dbp_command = 'env; wget -O dockerpush.py https://<github-repo>/ahunnargikar/elasticsearchindex/raw/master/loadtest/dockerpush.py; chmod +x dockerpush.py; python dockerpush.py;'
dbp_command = 'env; wget -O dockerpush.py https://<github-repo>/ahunnargikar/github_elasticsearchindex/raw/master/loadtest/dockerpush.py; chmod +x dockerpush.py; python dockerpush.py;'
dbp_constraints = [["clusterType", "LIKE", "elasticsearch3"]]
#dbp_constraints = []
dbp_cpus = 0.1
dbp_env = {'SIZE_RANGE_START': 2, 'SIZE_RANGE_END': 3}
dbp_executor = ''
dbp_instances = 1
dbp_memory = 64
dbp_ports = []
dbp_uris = []

#########################
##HAPROXY CONFIG
#########################
ha_clusterName = 'haproxy-' + randomString
ha_command = 'env; wget http://<github-repo>/ahunnargikar/haproxy-mesos/archive/master.zip; unzip master.zip; cd haproxy-mesos-master; python haproxy.py $REGISTRY_ADDR; sed -i "/bind :443/d" haproxy.cfg; sed -i "/bind :80/d" haproxy.cfg; sed -i "s#5000#$PORT0#g" haproxy.cfg; chmod +x haproxy; ./haproxy -db -f haproxy.cfg;'
ha_constraints = [["clusterType", "LIKE", "elasticsearch3"]]
ha_cpus = 1
ha_env = {}
ha_executor = ''
ha_instances = 1
ha_memory = 64
ha_ports = [0]
ha_uris = []