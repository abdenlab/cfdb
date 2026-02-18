import os
from typing import Final

from motor.motor_asyncio import AsyncIOMotorDatabase

DATABASE_URL: Final = os.getenv("DATABASE_URL", "mongodb://127.0.0.1:27017")
DATABASE_NAME: Final = os.getenv("DATABASE_NAME", "cfdb")
PAGE_SIZE: Final = 25

# TLS authentication configuration (production)
MONGODB_TLS_ENABLED: Final = os.getenv("MONGODB_TLS_ENABLED", "false").lower() == "true"
MONGODB_CA_PATH: Final = os.getenv(
    "MONGODB_CA_PATH", "/etc/cfdb/certs/global-bundle.pem"
)
MONGODB_RETRY_WRITES: Final = os.getenv("MONGODB_RETRY_WRITES", "false").lower() == "true"

# Sync API authentication
SYNC_API_KEY: Final = os.getenv("SYNC_API_KEY", "")

db: AsyncIOMotorDatabase | None = None
