from datetime import datetime
from elasticsearch import Elasticsearch
from app.logger import timed

@timed
def el_test():
    port = 34523
    links = ['http://localhost:9200', f"http://bore.pub:{port}/"]

    es = Elasticsearch(links[1])

    # # create an index (if it doesn't exist already)
    # es.indices.create(index='my_index')

    doc1 = {"title": "Document 1", "content": "This is the content of document 1"}
    doc2 = {"title": "Document 2", "content": "This is the content of document 2"}
    es.index(index='my_index', document=doc1)
    es.index(index='my_index', document=doc2)
    res = es.search(index='my_index', query={"match_all": {}})
    print(res['hits'])


if __name__=="__main__":
    el_test()