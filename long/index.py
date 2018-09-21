# -*- coding:utf-8 -*-

mapping_body = {
    "settings": {
        "index.number_of_shards": 8,
        "number_of_replicas": 1
    },
    "mappings": {
        "logs": {
            "properties": {
                "user_id": {
                    "type": "keyword"
                },
                "ctinfo": {
                    "type": "keyword"
                },
                "ct": {
                    "type": "keyword"
                },
                "msisdn": {
                    "type": "keyword"
                },
                "client_id": {
                    "type": "keyword"
                },
                "search_id": {
                    "type": "keyword"
                },
                "contDuration": {
                    "type": "integer"
                },
                "contDisplayType": {
                    "type": "keyword"
                },
                "mediaShape": {
                    "type": "keyword"
                },
                "contRecomm": {
                    "type": "keyword"
                },
                "mediaArea": {
                    "type": "keyword"
                },
                "mediaType": {
                    "type": "keyword"
                },
                "packId": {
                    "type": "keyword"
                },
                "publicTime": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                },
                "createTime": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                },
                "timeStamp": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                },
                "timeLine": {
                    "type": "date"
                },
                "timeLocal": {
                    "type": "date"
                },
                "search_ids": {
                    "type": "keyword"
                },
                "click": {
                    "type": "integer"
                },
                "source": {
                    "type": "keyword"
                },
                "watch_time": {
                    "type": "integer"
                }
            }
        }
    }
}
