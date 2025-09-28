ÖNEMLİ:

python -m app.db.init_db
python -m app.db.seeder



kurulum:

docker network create es-net

docker run -d --name es01 --net es-net \
  -p 9200:9200 -e "discovery.type=single-node" \
  docker.elastic.co/elasticsearch/elasticsearch:8.15.0

docker run -d --name kib01 --net es-net \
  -p 5601:5601 \
  docker.elastic.co/kibana/kibana:8.15.0




