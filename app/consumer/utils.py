def create_indices(es):
    indices = {
        "reviews": {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {"analyzer": {"english_analyzer": {"type": "english"}}},
            },
            "mappings": {
                "properties": {
                    "user_id": {"type": "integer"},
                    "product_id": {"type": "integer"},
                    "rating": {"type": "float"},
                    "review_text": {
                        "type": "text",
                        "analyzer": "english_analyzer",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
                    "timestamp": {
                        "type": "date",
                        "format": "strict_date_optional_time||epoch_millis",
                    },
                }
            },
        },
        "products": {
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {
                "properties": {
                    "product_id": {"type": "integer"},
                    "name": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
                    "total_ratings": {"type": "float"},
                    "total_reviews": {"type": "integer"},
                    "avg_rating": {"type": "float"},
                }
            },
        },
        "users": {
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {
                "properties": {
                    "user_id": {"type": "integer"},
                    "name": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
                    "email": {"type": "keyword"},
                    "phone": {"type": "keyword"},
                    "password": {
                        "type": "keyword",
                        "index": False,
                        "doc_values": False,
                    },
                    "total_ratings": {"type": "float"},
                    "total_reviews": {"type": "integer"},
                    "avg_rating": {"type": "float"},
                }
            },
        },
    }

    for name, body in indices.items():
        try:
            if not es.indices.exists(index=name):
                es.indices.create(index=name, body=body)
                print(f"Created index '{name}'")
            else:
                print(f"'{name}' index exists")
        except Exception as e:
            print(f"Error creating '{name}' index: {e}")
