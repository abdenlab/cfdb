import logging
import re
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from motor.motor_asyncio import AsyncIOMotorClient
from strawberry.fastapi import GraphQLRouter

from cfdb import api
from cfdb.api.gql.schema import schema
from cfdb.api.routers.data import router as data_router
from cfdb.api.routers.index import router as index_router
from cfdb.api.routers.sync import router as sync_router

logging.basicConfig(level=logging.INFO)


def redact_url(url: str) -> str:
    """Redact password from a MongoDB connection string for safe logging."""
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", url)


def create_mongodb_client() -> AsyncIOMotorClient:
    """Create MongoDB client with optional TLS authentication."""
    kwargs: dict = {}

    if not api.MONGODB_RETRY_WRITES:
        kwargs["retryWrites"] = False

    if api.MONGODB_TLS_ENABLED:
        print(f"Connecting to MongoDB at {redact_url(api.DATABASE_URL)} with TLS")
        return AsyncIOMotorClient(
            api.DATABASE_URL,
            tls=True,
            tlsCAFile=api.MONGODB_CA_PATH,
            **kwargs,
        )
    print(f"Connecting to MongoDB at {api.DATABASE_URL} (no authentication)")
    return AsyncIOMotorClient(api.DATABASE_URL, **kwargs)


@asynccontextmanager
async def lifespan(_: FastAPI):
    if not api.SYNC_API_KEY:
        logging.getLogger(__name__).warning(
            "SYNC_API_KEY not set — sync endpoint is unprotected"
        )

    client = create_mongodb_client()
    api.db = client[api.DATABASE_NAME]
    yield
    client.close()


app = FastAPI(lifespan=lifespan)
app.include_router(GraphQLRouter(schema), prefix="/metadata")
app.include_router(data_router)
app.include_router(index_router)
app.include_router(sync_router)


@app.get("/health")
async def health():
    return JSONResponse({"status": "ok"})
