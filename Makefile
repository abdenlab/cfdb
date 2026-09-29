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
	@# database/ is gitignored and empty on a clean checkout. It is a
	@# drop-in point for an optional mongodump, mounted read-only rather
	@# than baked into the image (see Dockerfile.mongodb). Created here so
	@# the mount source always exists and so the drop-in point is visible.
	@mkdir -p database
	@echo "Starting MongoDB container..."
	docker run -d --name mongodb --network cvh-backend-network --network-alias cvh-backend -p 27017:27017 -v "$(CURDIR)/database:/data/database:ro" cfdb-mongodb
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

# --- Local-dev matrix tile serving (issue #82) -----------------------
#
# `make api` builds an image with no clodius, so its tile routes answer
# 501 -- correct for production while the `tiles` extra stays optional,
# but it leaves the whole tile chain untestable through the documented
# Docker flow. These targets layer clodius on (installed from the public
# fork at the sha pyproject.toml pins) and add the piece `make api` does
# not need: a cache volume shared with a worker, since a tileset
# artifact is built by the worker and then opened by the API.
#
# Retire both once `tiles` becomes an ordinary dependency (raising the
# Python floor to 3.12) -- `make api` then covers this on its own.

api-tiles:
	make network
	@docker stop api 2>/dev/null || true
	@docker rm api 2>/dev/null || true
	@echo "Building the API image..."
	docker build -t api -f Dockerfile.api .
	@echo "Layering the clodius tile backend on top..."
	docker build -t api-tiles -f Dockerfile.api-tiles .
	@# The image runs as `app`, but a freshly created named volume is
	@# owned by root, so the lifespan's mkdir under SYNC_DATA_DIR fails
	@# with PermissionError and the API 500s on every workflow route.
	@docker volume create cfdb-sync >/dev/null
	@docker run --rm --user 0 --entrypoint chown -v cfdb-sync:/tmp/sync-data api-tiles -R app:app /tmp/sync-data
	@echo "Starting the API container with matrix tile serving enabled..."
	docker run -d --name api --network cvh-backend-network --network-alias cvh-backend -p 8000:8000 \
		-e SYNC_DATA_DIR=/tmp/sync-data \
		-e WORKFLOW_POOL_NAMESPACE=$${WORKFLOW_POOL_NAMESPACE:-cfdb-workers} \
		-v cfdb-sync:/tmp/sync-data \
		api-tiles
	@echo "API up on port 8000."
	@# Start the worker FIRST next time: the API dispatches on request, and a
	@# dispatch that finds no worker is queued rather than failed, so the job
	@# then waits out CFDB_WORKFLOW_RETRY_INTERVAL_S (2 min) before running.
	@echo "Run 'make worker-tiles' before POSTing a tileset, or the first job waits ~2 min for the retry tick."

# The worker that builds tileset artifacts. It reuses the API image
# rather than `cfdb-wool`, because the matrix processor needs cooler and
# h5py, which arrive with clodius and are absent from the worker image.
# It shares the API's cache volume: the worker writes the artifact, the
# API opens it locally with h5py.
worker-tiles:
	make network
	@docker stop worker 2>/dev/null || true
	@docker rm worker 2>/dev/null || true
	@echo "Starting a containerized LAN worker pool (namespace=$${WORKFLOW_POOL_NAMESPACE:-cfdb-workers})..."
	docker run -d --name worker --network cvh-backend-network \
		-e SYNC_DATA_DIR=/tmp/sync-data \
		-e WORKFLOW_POOL_NAMESPACE=$${WORKFLOW_POOL_NAMESPACE:-cfdb-workers} \
		-e WORKFLOW_WORKER_COUNT=$${WORKFLOW_WORKER_COUNT:-1} \
		-v cfdb-sync:/tmp/sync-data \
		api-tiles python -m cfdb.workflows.worker_lan
	@echo "Worker container is up. Check logs with: docker logs -f worker"

schema:
	@echo "Regenerating schema.graphql from the Strawberry schema..."
	uv run python scripts/export_schema.py

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
