===========
Elasticsearch Index
===========

The Elasticsearchindex package is meant to enable the use of Elasticsearch as an alternative search
backend instead of the default SQLAlchemy ORM. It can be enabled by adding the following in
the Docker registry config.yml file:

    search_backend: elasticsearchindex.elasticsearchindex
    search_options:
        address:
            http://<elasticsearch-server-address>:9200
        index:
            docker_registry
        type:
            layers

The Elasticsearch index package will store the repository metadata into an index called "docker_registry". Here's
how the images can be queried from Elasticsearch for a repository called "paas":

##GET ALL THE IMAGES IN MY REPOSITORY
curl -X POST "http://docker3:31586/docker_registry/layers/_search" -d'
{
    "query": {
        "bool" : {
            "must" : [ 
                { "match": { "namespace" : "paas" } },
                { "match": { "isHead" : "true" } }
            ]
         }
    }
}'