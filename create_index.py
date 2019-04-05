from elasticsearch import Elasticsearch

from utility import logger


def create_indices(event=None, context=None):
    INDEX_NAME = "test"
    es = Elasticsearch([{"host": "search-od-happy-face-5i3vty7clvmiz3cvgus3n6dfpa.us-east-1.es.amazonaws.com",
                         "port": 80}])

    if es.indices.exists(INDEX_NAME):
        logger.info("deleting '%s' index..." % (INDEX_NAME))
        res = es.indices.delete(index=INDEX_NAME)
        logger.info(" response: '%s'" % (res))

    request_body = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        'mappings': {
            'result': {
                'properties': {
                    "date": {
                        "type": "date",
                        "format": "YYYY-MM-DD H:mm:ss",
                        "store": True
                    },

                    'image_uuid': {
                        "type": "text",
                        "fields": {
                            "raw": {
                                "type": "keyword"
                            }
                        }
                    },
                    'path': {
                        "type": "text",
                        "fields": {
                            "rawPath": {
                                "type": "keyword"
                            }
                        }
                    },
                    'best_path': {
                        "type": "text",
                        "fields": {
                            "rawBestPath": {
                                "type": "keyword"
                            }
                        }
                    },
                    'confidence': {'type': 'float'},
                }
            }
        }
    }

    logger.info("creating '%s' index..." % (INDEX_NAME))
    res = es.indices.create(index=INDEX_NAME, body=request_body)
    logger.info(res)


create_indices()
