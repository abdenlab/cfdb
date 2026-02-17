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
