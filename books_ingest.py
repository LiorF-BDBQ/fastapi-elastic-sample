import csv

from elasticsearch import Elasticsearch, helpers

es = Elasticsearch(hosts="http://127.0.0.1:9200")
file_path = "books.csv"
index_name = "books"


def transform_row(row):
    cols = ['bookID', 'title', 'authors', 'average_rating', 'isbn', 'isbn13', 'language_code', 'num_pages',
            'ratings_count', 'text_reviews_count', 'publication_date', 'publisher']
    doc = {}
    for i in range(len(cols)):
        doc[cols[i]] = row[i]
    return doc


def document_stream(file_to_index):
    with open(file_to_index, "r") as csvfile:
        for row in csv.reader(csvfile):
            yield {"_index": index_name,
                   "_source": transform_row(row)
                   }


stream = document_stream(file_path)
for ok, response in helpers.streaming_bulk(es, actions=stream):
    if not ok:
        # Failure inserting
        print(response)
