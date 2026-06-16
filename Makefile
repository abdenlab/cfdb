network:
	@echo "Checking if Docker network 'cvh-backend-network' exists..."
	@if ! docker network inspect cvh-backend-network >/dev/null 2>&1; then \
		echo "Creating Docker network 'cvh-backend-network'..."; \
		docker network create cvh-backend-network; \
	else \
		echo "Network cvh-backend-network already exists."; \
	fi

mongodb:
	make network
	@docker stop mongodb 2>/dev/null || true
	@docker rm mongodb 2>/dev/null || true
	@echo "Building MongoDB image..."
	docker build -t cfdb-mongodb -f Dockerfile.mongodb .
	@echo "Starting MongoDB container..."
	docker run -d --name mongodb --network cvh-backend-network --network-alias cvh-backend -p 27017:27017 cfdb-mongodb
	@echo "MongoDB container starting on port 27017. Check logs with: docker logs -f mongodb"

build-materialize:
	@echo "Building materializer..."
	cd materialize && cargo build --release
	@echo "Materializer built at materialize/target/release/materialize"

install-materialize: build-materialize
	@echo "Installing materializer to /usr/local/bin..."
	sudo cp materialize/target/release/materialize /usr/local/bin/
	@echo "Materializer installed."

materialize-files: build-materialize
	@echo "Materializing 'files' collection..."
	./materialize/target/release/materialize
	@echo "Files collection created successfully."

materialize-dcc: build-materialize
	@echo "Materializing file metadata for $(DCC)..."
	./materialize/target/release/materialize --submission $(DCC)
	@echo "Done."

api:
	make network
	@docker stop api 2>/dev/null || true
	@docker rm api 2>/dev/null || true
	@echo "Building the API Docker image..."
	docker build -t api -f Dockerfile.api .
	@echo "Starting the API container in DEVELOPMENT mode (no TLS)..."
	docker run -d --name api --network cvh-backend-network --network-alias cvh-backend -p 8000:8000 -e SYNC_DATA_DIR=/tmp/sync-data api
	@echo "API container is up and running on port 8000 (http://0.0.0.0:8000/metadata)."

wool:
	@echo "Building the wool worker Docker image (cfdb-wool, linux/amd64)..."
	docker build --platform linux/amd64 -t cfdb-wool -f Dockerfile.wool .
	@echo "Worker image built. CMD is 'python -m cfdb.workflows.worker_main' (ECS entrypoint)."

worker-local:
	@echo "Starting a local LAN worker pool (namespace=$${WORKFLOW_POOL_NAMESPACE:-cfdb-workers}, workers=$${WORKFLOW_WORKER_COUNT:-2})..."
	uv run python -m cfdb.workflows.worker_lan

worker-certs:
	@echo "Generating wool worker mutual-TLS material under certs/..."
	./certs/generate-worker-certs.sh
	@echo "Done. Export the cert paths on BOTH the worker pool and the API to enable mTLS:"
	@echo "  export CFDB_WORKER_TLS_CA=certs/worker-ca/ca.pem"
	@echo "  # worker pool:  CFDB_WORKER_TLS_CERT=certs/worker/worker-cert.pem CFDB_WORKER_TLS_KEY=certs/worker/worker-key.pem"
	@echo "  # API:          CFDB_WORKER_TLS_CERT=certs/api/api-cert.pem       CFDB_WORKER_TLS_KEY=certs/api/api-key.pem"

# Local LAN worker pool with mutual TLS enabled. Run `make worker-certs`
# first; start the API with the matching CFDB_WORKER_TLS_CA plus the API
# leaf cert/key so both sides authenticate against the shared CA.
worker-local-tls:
	@echo "Starting a local LAN worker pool with mTLS (namespace=$${WORKFLOW_POOL_NAMESPACE:-cfdb-workers}, workers=$${WORKFLOW_WORKER_COUNT:-2})..."
	CFDB_WORKER_TLS_CA=certs/worker-ca/ca.pem \
	CFDB_WORKER_TLS_CERT=certs/worker/worker-cert.pem \
	CFDB_WORKER_TLS_KEY=certs/worker/worker-key.pem \
	uv run python -m cfdb.workflows.worker_lan
