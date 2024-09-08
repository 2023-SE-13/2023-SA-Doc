curl -X POST "http://elastic:yXC0ZTAbjmhmyLHb7fBv@localhost:9200/_reindex?timeout=1720m" -H "Content-Type: application/json" -d'
{
  "source": {
    "index": "works", 
    "size": 10000
  },
  "dest": {
    "index": "new_works"
  }
}'
