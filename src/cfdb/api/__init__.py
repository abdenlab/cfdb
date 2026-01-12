import os
from typing import Final

from motor.motor_asyncio import AsyncIOMotorDatabase

DATABASE_URL: Final = os.getenv("DATABASE_URL", "mongodb://127.0.0.1:27017")
DATABASE_NAME: Final = os.getenv("DATABASE_NAME", "cfdb")
PAGE_SIZE: Final = 25

# TLS/X.509 authentication configuration (production)
MONGODB_TLS_ENABLED: Final = os.getenv("MONGODB_TLS_ENABLED", "false").lower() == "true"
MONGODB_CERT_PATH: Final = os.getenv("MONGODB_CERT_PATH", "/etc/cfdb/certs/client-bundle.pem")
MONGODB_CA_PATH: Final = os.getenv("MONGODB_CA_PATH", "/etc/cfdb/certs/ca.pem")

db: AsyncIOMotorDatabase | None = None
