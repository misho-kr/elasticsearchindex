#! /usr/bin/python

__author__="Ashish Hunnargikar"
__date__ ="$Jun 13, 2014 12:33:33 PM$"

import urllib2
import json
import time
import httplib
import sys

if sys.argv[1] == 'qa':
    import qaconfig as config
else:
    import devconfig as config

def launch_es_cluster(marathonUrl, es_command, es_constraints, es_cpus, es_env, es_executor, es_clusterName, es_instances, es_memory, es_ports, es_uris):
    """
    Launches an Elasticsearch cluster in Mesos using Marathon APIs

    @return: JSON response from Marathon REST APIs
    """
    payload = {}
    payload['cmd'] = es_command
    payload['constraints'] = es_constraints
    payload['cpus'] = es_cpus
    #payload['env'] = es_env
    envs = {}
    payload['env'] = dict(list(es_env.items()) + list(envs.items()))
    payload['executor'] = es_executor
    payload['id'] = es_clusterName
    payload['instances'] = es_instances
    payload['mem'] = es_memory
    payload['ports'] = es_ports
    payload['uris'] = es_uris

    try:
        print("*************************ES PAYLOAD*****************************")
        print(json.dumps(payload))
        print("****************************************************************")
        req = urllib2.Request(marathonUrl, json.dumps(payload), headers={'User-Agent': 'loadtest-client', 'Content-Type': 'application/json'})
        response = urllib2.urlopen(req).read()
        return json.loads(response)
    except ValueError:
        return None

def get_es_cluster_addresses(marathonUrl, es_clusterName, es_instances):
    """
    Waits for all the Elasticsearch nodes to come up and then collects the node address and port information using
    the Elasticsearch Cat API

    @return: JSON response from the Elasticsearch Cat API with the list of nodes
    """
    for x in range(0, 100):
            response = get_cluster_info_from_marathon(marathonUrl, es_clusterName)
            length = len(response['tasks'])

            #Wait until all the N nodes in this cluster are up and running
            if (length == es_instances):

                url = ''
                #Get the address of the first node that came up in Marathon and use its Cat API to find the other nodes
                for node in response['tasks']:
                    url = "http://" + node['host'] + ":" + str(node['ports'][0]) + "/_cat/nodes"

                #Keep on requesting the cluster node information until its available
                response = None
                count = 0
                while (count < es_instances):
                    req1 = urllib2.Request(url, headers={'User-Agent': 'loadtest-client', 'Content-Type': 'application/json'})
                    try:
                        response = urllib2.urlopen(req1).read()
                        response = json.loads(response)
                        count = len(response)
                        print('Checking ES cluster status...... ' + str(count) + '/' + es_instances + ' ES cluster nodes are now fully active!')
                    except urllib2.HTTPError, e:
                        print("ES urllib2.HTTPError")
                        pass
                    except urllib2.URLError, e:
                        print("ES urllib2.URLError")
                        pass
                    except httplib.HTTPException, e:
                        print("ES httplib.HTTPException")
                        pass
                    except Exception:
                        print("ES Exception")
                        pass
                    time.sleep(5)
                #print("#########RESPONSE :: " + json.dumps(response))
                return response

            #Sleep for 5 secs before requesting the status of the ES cluster again
            time.sleep(5)

def get_es_nodeinfo_from_marathon(marathonUrl, es_clusterName):
    """
    Queries Marathon REST APIs for the Elasticsearch cluster nodes running in Mesos. The Elasticsearch cluster
    app name to query for i available from the global config file.

    @return: JSON response from Marathon REST APIs with the list of app tasks and other associated information
    """
    response = None
    try:
        url = marathonUrl + es_clusterName + "/tasks"
        req = urllib2.Request(url, headers={'User-Agent': 'loadtest-client', 'Content-Type': 'application/json', 'Accept': 'application/json'})
        response = urllib2.urlopen(req).read()
        response = json.loads(response)
        #print("#########MARATHON RESPONSE :: " + json.dumps(response))
    except urllib2.HTTPError, e:
        #print("urllib2.HTTPError")
        pass
    except urllib2.URLError, e:
        #print("urllib2.URLError")
        pass
    except httplib.HTTPException, e:
        #print("httplib.HTTPException")
        pass
    except Exception:
        #print("Exception")
        pass
    return response

#def launch_dr_cluster(marathonUrl, zookeeperAddr, es_clusterName, es_index, dr_command, dr_constraints, dr_cpus, dr_executor, dr_clusterName, dr_instances, dr_memory, dr_ports, dr_uris):
#    """
#    Launches a Docker registry cluster in Mesos using Marathon APIs
#
#    @return: JSON response from Marathon REST APIs
#    """
#    payload = {}
#    payload['cmd'] = dr_command
#    payload['constraints'] = dr_constraints
#    payload['cpus'] = dr_cpus
#    payload['env'] = {'ZK_ADDR': zookeeperAddr, 'ES_ADDR': es_clusterName, 'ES_INDEX': es_index}
#    payload['executor'] = dr_executor
#    payload['id'] = dr_clusterName
#    payload['instances'] = dr_instances
#    payload['mem'] = dr_memory
#    payload['ports'] = dr_ports
#    payload['uris'] = dr_uris
#
#    try:
#        req = urllib2.Request(marathonUrl, json.dumps(payload), headers={'User-Agent': 'loadtest-client', 'Content-Type': 'application/json'})
#        response = urllib2.urlopen(req).read()
#        response =  json.loads(response)
#        return response
#    except urllib2.HTTPError, e:
#        print("Docker registry urllib2.HTTPError")
#        pass
#    except urllib2.URLError, e:
#        print("Docker registry urllib2.URLError")
#        pass
#    except httplib.HTTPException, e:
#        print("Docker registry httplib.HTTPException")
#        pass
#    except Exception:
#        #print("Docker registry Exception")
#        pass
#    return response

def launch_dr_cluster(marathonUrl, zookeeperAddr, es_clusterName, es_index, dr_constraints, dr_cpus, dr_clusterName, dr_instances, dr_memory, dr_ports, dr_uris, dr_env, dr_container):
    """
    Launches a Docker registry cluster in Mesos using Marathon APIs

    @return: JSON response from Marathon REST APIs
    """
    payload = {}
    payload['constraints'] = dr_constraints
    payload['container'] = dr_container
    payload['cpus'] = dr_cpus
    envs = {'ZK_ADDRESS': zookeeperAddr, 'ES_CLUSTER': es_clusterName, 'ES_INDEX': es_index}
    payload['env'] = dict(list(dr_env.items()) + list(envs.items()))
    payload['id'] = dr_clusterName
    payload['instances'] = dr_instances
    payload['mem'] = dr_memory
    payload['ports'] = dr_ports
    payload['uris'] = dr_uris

    try:
        print("*************************REGISTRY PAYLOAD*****************************")
        print(json.dumps(payload))
        print("****************************************************************")
        req = urllib2.Request(marathonUrl, json.dumps(payload), headers={'User-Agent': 'loadtest-client', 'Content-Type': 'application/json'})
        response = urllib2.urlopen(req).read()
        response =  json.loads(response)
        return response
    except urllib2.HTTPError, e:
        print("Docker registry urllib2.HTTPError")
        pass
    except urllib2.URLError, e:
        print("Docker registry urllib2.URLError")
        pass
    except httplib.HTTPException, e:
        print("Docker registry httplib.HTTPException")
        pass
    except Exception:
        #print("Docker registry Exception")
        pass
    return response
    
def get_dr_cluster_addresses(marathonUrl, dr_clusterName, dr_instances):
    """
    Retrieves the list of Docker Registry nodes from the Marathon REST APIs

    @return: JSON response from Marathon REST APIs containing the Docker registry node information
    """
    for x in range(0, 100):
            response = get_cluster_info_from_marathon(marathonUrl, dr_clusterName)
            length = len(response['tasks'])

            #Wait until all the N nodes in this cluster are up and running
            if (length == dr_instances):

                url = ''
                #Query the registry nodes to see if they're alive yet
                count = 0;
                for node in response['tasks']:
                    url = 'http://' + node['host'] + ':' + str(node['ports'][0]) + '/v1/_ping'

                    #Keep on requesting the cluster node information until its available
                    req1 = urllib2.Request(url, headers={'User-Agent': 'loadtest-client', 'Content-Type': 'application/json'})
                    try:
                        res1 = urllib2.urlopen(req1).read()
                        if (res1 == 'true'):
                            count = count + 1
                            print(str(count) + '/' + dr_instances + ' Docker Registry instances are now up and running!')
                    except urllib2.HTTPError, e:
                        print("DR urllib2.HTTPError")
                        pass
                    except urllib2.URLError, e:
                        print("DR urllib2.URLError")
                        pass
                    except httplib.HTTPException, e:
                        print("DR httplib.HTTPException")
                        pass
                    except Exception:
                        print("DR Exception")
                        pass

                if (count == dr_instances):
                    return response

            #Sleep for 5 secs before requesting the status of the ES cluster again
            time.sleep(5)

def get_haproxy_cluster_addresses(marathonUrl, ha_clusterName, ha_instances):
    """
    Retrieves the list of HAProxy nodes from the Marathon REST APIs

    @return: JSON response from Marathon REST APIs containing the Docker registry node information
    """
    for x in range(0, 100):
            response = get_cluster_info_from_marathon(marathonUrl, ha_clusterName)
            length = len(response['tasks'])

            #Wait until all the N nodes in this cluster are up and running
            if (length == ha_instances):
                return response
            time.sleep(5)

def get_cluster_info_from_marathon(marathonUrl, clusterName):
    """
    Retrieves the app Mesos task information from the Marathon REST APIs

    @return: JSON response from Marathon REST APIs containing the task information
    """
    try:
        req = urllib2.Request(marathonUrl + clusterName + "/tasks", headers={'User-Agent': 'loadtest-client', 'Content-Type': 'application/json', 'Accept': 'application/json'})
        response = urllib2.urlopen(req).read()
        return json.loads(response)
    except ValueError:
        return None

def launch_dbp_cluster(marathonUrl, registryAddr, es_clusterName, zookeeperAddr, x, dbp_command, dbp_constraints, dbp_cpus, dbp_executor, dbp_clusterName, dbp_memory, dbp_ports, dbp_uris, dbp_env, es_index):
    """
    Launches the Docker build-push-pull client

    @return: JSON response from Marathon REST APIs containing the task information
    """
    payload = {}
    payload['cmd'] = dbp_command
    payload['constraints'] = dbp_constraints
    payload['cpus'] = dbp_cpus
    envs = {"REGISTRY_ADDR": registryAddr, 'ES_CLUSTER': es_clusterName, 'ZK_ADDRESS': zookeeperAddr, 'ES_INDEX': es_index}
    payload['env'] = dict(list(dbp_env.items()) + list(envs.items()))
    payload['executor'] = dbp_executor
    payload['id'] = dbp_clusterName + registryAddr.replace(':','') + str(x)
    payload['instances'] = 1
    payload['mem'] = dbp_memory
    payload['ports'] = dbp_ports
    payload['uris'] = dbp_uris

    response = None
    try:
        print("*************************DBP PAYLOAD*****************************")
        print(json.dumps(payload))
        print("****************************************************************")
        req = urllib2.Request(marathonUrl, json.dumps(payload), headers={'User-Agent': 'loadtest-client', 'Content-Type': 'application/json'})
        response = urllib2.urlopen(req).read()
        response =  json.loads(response)
        return response
    except urllib2.HTTPError, e:
        print("DBP urllib2.HTTPError")
        pass
    except urllib2.URLError, e:
        print("DBP urllib2.URLError")
        pass
    except httplib.HTTPException, e:
        print("DBP httplib.HTTPException")
        pass
    except Exception:
        #print("DBP Exception")
        pass
    return response

def launch_haproxy_cluster(marathonUrl, registryAddr, zookeeperAddr, ha_command, ha_constraints, ha_cpus, ha_executor, ha_clusterName, ha_instances, ha_memory, ha_ports, ha_uris, ha_env):
    """
    Launches the HAProxy load balancer cluster for the Docker registry instances

    @return: JSON response from Marathon REST APIs containing the app information
    """
    payload = {}
    payload['cmd'] = ha_command
    payload['constraints'] = ha_constraints
    payload['cpus'] = ha_cpus
    envs = {'ZK_ADDRESS': zookeeperAddr, 'REGISTRY_ADDR': registryAddr}
    payload['env'] = dict(list(ha_env.items()) + list(envs.items()))
    payload['executor'] = ha_executor
    payload['id'] = ha_clusterName
    payload['instances'] = ha_instances
    payload['mem'] = ha_memory
    payload['ports'] = ha_ports
    payload['uris'] = ha_uris

    response = None
    try:
        print("*************************HAPROXY PAYLOAD*****************************")
        print(json.dumps(payload))
        print("****************************************************************")
        req = urllib2.Request(marathonUrl, json.dumps(payload), headers={'User-Agent': 'loadtest-client', 'Content-Type': 'application/json'})
        response = urllib2.urlopen(req).read()
        response =  json.loads(response)
        return response
    except urllib2.HTTPError, e:
        print("DBP urllib2.HTTPError")
        pass
    except urllib2.URLError, e:
        print("DBP urllib2.URLError")
        pass
    except httplib.HTTPException, e:
        print("DBP httplib.HTTPException")
        pass
    except Exception:
        #print("DBP Exception")
        pass
    return response

def orchestrate_load_test():
    """
    Orchestrate the load test

    """
    #1. Launch the Elasticsearch cluster
    launch_es_cluster(config.marathonUrl, config.es_command, config.es_constraints, config.es_cpus, config.es_env, config.es_executor, config.es_clusterName, config.es_instances, config.es_memory, config.es_ports, config.es_uris)

    #2. Get the addresses of all the Elasticsearch nodes
    esNodes = get_es_cluster_addresses(config.marathonUrl, config.es_clusterName, config.es_instances)

    #3. Get the list of ip addresses and ports from Marathon
    esMarathonNodes = get_es_nodeinfo_from_marathon(config.marathonUrl, config.es_clusterName)
    esNodeList = []
    for item in esMarathonNodes['tasks']:
        esNodeList.append(item['host'] + ":" + str(item['ports'][0]))
    print("ES cluster name :: " + config.es_clusterName)
    print("ES cluster node list :: " + json.dumps(esNodeList))

    #4. Launch the Docker registry and pass in the Marathon URL for the cluster
    print("Marathon URL used to pick ramdom ES nodes :: " + config.marathonUrl + config.es_clusterName + "/tasks")
    #launch_dr_cluster(config.marathonUrl, config.zookeeperAddr, config.es_clusterName, config.es_index, config.dr_command, config.dr_constraints, config.dr_cpus, config.dr_executor, config.dr_clusterName, config.dr_instances, config.dr_memory, config.dr_ports, config.dr_uris)
    launch_dr_cluster(config.marathonUrl, config.zookeeperAddr, config.es_clusterName, config.es_index, config.dr_constraints, config.dr_cpus, config.dr_clusterName, config.dr_instances, config.dr_memory, config.dr_ports, config.dr_uris, config.dr_env, config.dr_container)

    #5. Get the addresses of all the Docker Registry nodes
    drMarathonNodes = get_dr_cluster_addresses(config.marathonUrl, config.dr_clusterName, config.dr_instances)
    drNodeList = []
    for item in drMarathonNodes['tasks']:
        registryNode = item['host'] + ":" + str(item['ports'][0])
        drNodeList.append(registryNode)
    print("Docker Registry Node List :: " + json.dumps(drNodeList))

    #6. Launch the HAProxy cluster and get the address and port to be provided to the clients
    launch_haproxy_cluster(config.marathonUrl, " ".join(drNodeList), config.zookeeperAddr, config.ha_command, config.ha_constraints, config.ha_cpus, config.ha_executor, config.ha_clusterName, config.ha_instances, config.ha_memory, config.ha_ports, config.ha_uris, config.ha_env)

    haNodes = get_haproxy_cluster_addresses(config.marathonUrl, config.ha_clusterName, config.ha_instances)
    haNode = ''
    for item in haNodes['tasks']:
        haNode = item['host'] + ":" + str(item['ports'][0])

    #7. Sleep before launching the load test clients
    time.sleep(10)
    for x in range(config.dbp_instances):
        print('Launching DBP load test client #' + str(x + 1) +' for HAProxy node : ' + haNode)
        launch_dbp_cluster(config.marathonUrl, haNode, config.es_clusterName, config.zookeeperAddr, x, config.dbp_command, config.dbp_constraints, config.dbp_cpus, config.dbp_executor, config.dbp_clusterName, config.dbp_memory, config.dbp_ports, config.dbp_uris, config.dbp_env, config.es_index)

if __name__ == "__main__":
    orchestrate_load_test()