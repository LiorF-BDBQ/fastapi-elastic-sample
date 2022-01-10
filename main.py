import uvicorn
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

es = AsyncElasticsearch(hosts="http://127.0.0.1:9200")


class Tweet(BaseModel):
    message: str


@app.get("/")
async def read_all():
    return await es.search(index="tweets")


@app.get("/{id}")
async def read_one(id: str):
    return None


@app.delete("/{id}")
async def delete_one(id: str):
    return None


@app.post("/{id}")
async def create_one(id: str, tweet: Tweet):
    print(tweet)
    return None


@app.post("/{id}")
async def update_one(id: str, tweet: Tweet):
    print(tweet)
    return None

if __name__ == "__main__":
    # Run uvicorn
    uvicorn.run(
        "main:app",
        reload=True,
    )

