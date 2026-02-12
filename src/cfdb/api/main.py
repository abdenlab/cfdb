import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from strawberry.fastapi import GraphQLRouter

from cfdb import api
from cfdb.api.gql.schema import schema
from cfdb.api.routers.data import router as data_router
from cfdb.api.routers.index import router as index_router
from cfdb.api.routers.sync import router as sync_router

logging.basicConfig(level=logging.INFO)


def create_mongodb_client() -> AsyncIOMotorClient:
    """Create MongoDB client with optional TLS/X.509 authentication."""
    if api.MONGODB_TLS_ENABLED:
        print(f"Connecting to MongoDB at {api.DATABASE_URL} with X.509 authentication")
        return AsyncIOMotorClient(
            api.DATABASE_URL,
            authMechanism="MONGODB-X509",
            tls=True,
            tlsCertificateKeyFile=api.MONGODB_CERT_PATH,
            tlsCAFile=api.MONGODB_CA_PATH,
            authSource="$external",
        )
    print(f"Connecting to MongoDB at {api.DATABASE_URL} (no authentication)")
    return AsyncIOMotorClient(api.DATABASE_URL)


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Validate required configuration
    if not api.SYNC_API_KEY:
        raise RuntimeError("SYNC_API_KEY environment variable is required")

    client = create_mongodb_client()
    api.db = client[api.DATABASE_NAME]
    yield
    client.close()


app = FastAPI(lifespan=lifespan)
app.include_router(GraphQLRouter(schema), prefix="/metadata")
app.include_router(data_router)
app.include_router(index_router)
app.include_router(sync_router)
