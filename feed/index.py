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
                "page_source": {
                    "type": "keyword"
                },
                "msisdn": {
                    "type": "keyword"
                },
                "client_id": {
                    "type": "keyword"
                },
                "channel": {
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
                "contDisplayName": {
                    "type": "keyword"
                },
                "mediaShape": {
                    "type": "keyword"
                },
                "contRecomm": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword"
                        }
                    }
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
                "publishTime": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                },
                "createTime": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                },
                "mediaTime": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                },
                "timeStamp": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                },
                "timeLocal": {
                    "type": "date"
                },
                "timeLine": {
                    "type": "date"
                },
                "click": {
                    "type": "integer"
                },
                "watch_time": {
                    "type": "integer"
                },
                "source": {
                    "type": "keyword"
                },
                "four_source": {
                    "type": "integer"
                },
                "tag": {
                    "type": "keyword"
                },
                "request": {
                    "type": "keyword"
                },
                "request_active": {
                    "type": "integer"
                },
                "CP_ID": {
                    "type": "keyword"
                },
                "profile": {
                    "type": "integer"
                },
                "video_source": {
                    "type": "keyword"
                },
                "keywords": {
                    "type": "text",
                    "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                    }
                },
                "register_user": {
                    "type": "integer"
                }
            }
        }
    }
}
