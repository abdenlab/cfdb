# Certificate directory (customize for production)
CERT_DIR ?= $(PWD)/certs

network:
	@echo "Checking if Docker network 'cvh-backend-network' exists..."
	@if ! docker network inspect cvh-backend-network >/dev/null 2>&1; then \
		echo "Creating Docker network 'cvh-backend-network'..."; \
		docker network create cvh-backend-network; \
	else \
		echo "Network cvh-backend-network already exists."; \
	fi

# Generate certificates for TLS/X.509 authentication
certs:
	@echo "Generating TLS certificates..."
	./certs/generate-certs.sh
	@echo "Certificates generated in $(CERT_DIR)"

# Development mode (no authentication)
mongodb:
	make network
	@echo "Building MongoDB image..."
	docker build -t cfdb-mongodb -f Dockerfile.mongodb .
	@echo "Starting MongoDB container in DEVELOPMENT mode (no TLS)..."
	docker run -d --name mongodb --network cvh-backend-network --network-alias cvh-backend -p 27017:27017 cfdb-mongodb
	@echo "MongoDB container starting on port 27017. Check logs with: docker logs -f mongodb"

# Production mode (TLS/X.509 authentication)
mongodb-prod:
	make network
	@echo "Building MongoDB image..."
	docker build -t cfdb-mongodb -f Dockerfile.mongodb .
	@echo "Starting MongoDB container in PRODUCTION mode (TLS/X.509)..."
	docker run -d --name mongodb \
		--network cvh-backend-network \
		--network-alias cvh-backend \
		-p 27017:27017 \
		-v $(CERT_DIR)/ca/ca.pem:/etc/mongodb/certs/ca.pem:ro \
		-v $(CERT_DIR)/server/cvh-backend-bundle.pem:/etc/mongodb/certs/server-bundle.pem:ro \
		cfdb-mongodb
	@echo "MongoDB container starting on port 27017 with TLS. Check logs with: docker logs -f mongodb"

build-materialize:
	@echo "Building materializer..."
	cd materialize && cargo build --release
	@echo "Materializer built at materialize/target/release/materialize"

install-materialize: build-materialize
	@echo "Installing materializer to /usr/local/bin..."
	sudo cp materialize/target/release/materialize /usr/local/bin/
	@echo "Materializer installed."

# Development mode (no authentication)
materialize-files: build-materialize
	@echo "Materializing 'files' collection (dev mode)..."
	./materialize/target/release/materialize
	@echo "Files collection created successfully."

materialize-dcc: build-materialize
	@echo "Materializing file metadata for $(DCC) (dev mode)..."
	./materialize/target/release/materialize --submission $(DCC)
	@echo "Done."

# Production mode (TLS/X.509 authentication)
materialize-files-prod: build-materialize
	@echo "Materializing 'files' collection (TLS/X.509)..."
	MONGODB_TLS_ENABLED=true \
	MONGODB_CERT_PATH=$(CERT_DIR)/clients/cfdb-materializer-bundle.pem \
	MONGODB_CA_PATH=$(CERT_DIR)/ca/ca.pem \
	DATABASE_URL=mongodb://cvh-backend:27017 \
	./materialize/target/release/materialize
	@echo "Files collection created successfully."

materialize-dcc-prod: build-materialize
	@echo "Materializing file metadata for $(DCC) (TLS/X.509)..."
	MONGODB_TLS_ENABLED=true \
	MONGODB_CERT_PATH=$(CERT_DIR)/clients/cfdb-materializer-bundle.pem \
	MONGODB_CA_PATH=$(CERT_DIR)/ca/ca.pem \
	DATABASE_URL=mongodb://cvh-backend:27017 \
	./materialize/target/release/materialize --submission $(DCC)
	@echo "Done."

# Development mode (no authentication)
api:
	make network
	@echo "Building the API Docker image..."
	docker build -t api -f Dockerfile.api .
	@echo "Starting the API container in DEVELOPMENT mode (no TLS)..."
	docker run -d --name api --network cvh-backend-network --network-alias cvh-backend -p 8000:8000 -e SYNC_API_KEY=dev-sync-key -e SYNC_DATA_DIR=/tmp/sync-data api
	@echo "API container is up and running on port 8000 (http://0.0.0.0:8000/metadata)."

# Production mode (TLS/X.509 authentication)
api-prod:
	make network
	@echo "Building the API Docker image..."
	docker build -t api -f Dockerfile.api .
	@echo "Starting the API container in PRODUCTION mode (TLS/X.509)..."
	docker run -d --name api \
		--network cvh-backend-network \
		--network-alias cvh-backend \
		-p 8000:8000 \
		-e MONGODB_TLS_ENABLED=true \
		-e DATABASE_URL=mongodb://cvh-backend:27017 \
		-e SYNC_API_KEY=$(SYNC_API_KEY) \
		-e SYNC_DATA_DIR=/tmp/sync-data \
		-v $(CERT_DIR)/ca/ca.pem:/etc/cfdb/certs/ca.pem:ro \
		-v $(CERT_DIR)/clients/cfdb-api-bundle.pem:/etc/cfdb/certs/client-bundle.pem:ro \
		api
	@echo "API container is up with TLS on port 8000 (http://0.0.0.0:8000/metadata)."
