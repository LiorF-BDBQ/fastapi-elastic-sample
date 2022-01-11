import csv
import gzip
import sys

from elasticsearch import Elasticsearch, helpers

es = Elasticsearch(hosts="http://127.0.0.1:9200")
file_path = "articles.csv.gz"
index_name = "articles"
csv.field_size_limit(sys.maxsize)


def transform_row(row):
    cols = ['index', 'id', 'title', 'publication', 'author', 'date', 'year', 'month', 'url', 'content']
    doc = {}
    for i in range(len(cols)):
        doc[cols[i]] = row[i]
    return doc


def document_stream(file_to_index):
    with gzip.open(file_to_index, "rt") as csvfile:
        for row in csv.reader(csvfile):
            yield {"_index": index_name,
                   "_source": transform_row(row)
                   }


stream = document_stream(file_path)
for ok, response in helpers.streaming_bulk(es, actions=stream):
    if not ok:
        # Failure inserting
        print(response)
