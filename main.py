from typing import List, Optional

import uvicorn
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

es = AsyncElasticsearch(hosts="http://localhost:9200")


class Tweet(BaseModel):
    message: str
    user: str
    client: str
    retweeted: bool
    source: str
    urls: List[str]


class OptionalTweet(BaseModel):
    message: Optional[str]
    user: Optional[str]
    client: Optional[str]
    retweeted: Optional[bool]
    source: Optional[str]
    urls: Optional[List[str]]


@app.get("/")
async def read_all():
    return await es.search(index="tweets")


@app.get("/{id}")
async def read_one(id: str):
    return await es.get(index="tweets", id=id)


@app.delete("/{id}")
async def delete_one(id: str):
    return await es.delete(index="tweets", id=id)


@app.post("/create")
async def create_one(tweet: Tweet):
    return await es.index(index="tweets", document=tweet.json())


@app.put("/{id}")
async def update_one(id: str, tweet: OptionalTweet):
    return await es.update(index="tweets", id=id, doc=tweet.dict(exclude_unset=True))


class SearchRequest(BaseModel):
    query: str
    publication: Optional[str]
    date_filter: Optional[dict]
    author: Optional[str]
    page: int = 1


def generate_fts_clause(request: SearchRequest) -> dict:
    return {
        "bool": {
            "should": [
                {
                    "multi_match": {
                        "fields": [
                            "title", "title.english"
                        ],
                        "boost": 50,
                        "type": "phrase",
                        "query": request.query
                    }
                },
                {
                    "multi_match": {
                        "fields": [
                            "content", "content.english"
                        ],
                        "boost": 2,
                        "slop": 4,
                        "type": "phrase",
                        "query": request.query
                    }
                },
                {
                    "multi_match": {
                        "fields": [
                            "content", "content.english", "content", "content.english"
                        ],
                        "query": request.query
                    }
                }
            ]
        }
    }


def generate_filters(request: SearchRequest) -> List[dict]:
    filters = []
    if request.author:
        filters.append({
            "match": {
                "author": request.author
            }
        })
    if request.date_filter:
        filters.append({
            "range": {
                "date": request.date_filter
            }
        })
    return filters


def calc_paging(page: int):
    return (page - 1) * 10


@app.post("/search")
async def search(request: SearchRequest):
    highlight, query = await build_query(request)
    aggs = {
        "authors": {
            "terms": {
                "field": "authors.keyword",
                "size": 50
            }
        }
    }
    suggest = {
        "did_you_mean": {
            "text": request.query,
            "phrase": {
                "field": "title"
            }
        }
    }

    return await es.search(index="books", query=query, _source=["title"], highlight=highlight,
                           from_=calc_paging(request.page), aggregations=aggs, suggest=suggest)


async def build_query(request):
    fts_clause = generate_fts_clause(request)
    filter_clauses = generate_filters(request)
    query = {
        "bool": {
            "must": fts_clause,
            "filter": filter_clauses
        }
    }
    highlight = {
        "fields": {
            "title": {}
        }
    }
    return highlight, query


@app.post("/author_search")
async def author_search(request: SearchRequest):
    highlight, query = await build_query(request)
    collapse = {
        "field": "authors.keyword",
        "inner_hits": {
            "name": "books_By_author",
            "_source": ["title"],
            "size": 3
        }
    }
    return await es.search(index="books", query=query, _source=False, highlight=highlight,
                           from_=calc_paging(request.page), collapse=collapse)



if __name__ == "__main__":
    # Run uvicorn
    uvicorn.run(
        "main:app",
        reload=True,
    )
