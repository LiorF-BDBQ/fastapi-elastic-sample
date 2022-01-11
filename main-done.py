from typing import List, Optional

import uvicorn
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

es = AsyncElasticsearch(hosts="http://ec2-35-158-96-222.eu-central-1.compute.amazonaws.com:9200")


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


@app.post("/")
async def create_one(tweet: Tweet):
    return await es.index(index="tweets", document=tweet.json())


@app.put("/{id}")
async def update_one(id: str, tweet: OptionalTweet):
    return await es.update(index="tweets", id=id, doc=tweet.dict(exclude_unset=True))

if __name__ == "__main__":
    # Run uvicorn
    uvicorn.run(
        "main:app",
        reload=True,
    )

