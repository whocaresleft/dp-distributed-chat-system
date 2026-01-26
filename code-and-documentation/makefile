# Build
all:
	./init.sh
	go build -o bootstrap ./cmd/bootstrap
	go build -o nameserver ./cmd/nameserver
	go build -o node ./cmd/node

# Clean up
clean:
	find ./NODE_* -type f ! -name "*.cfg" -delete
	find ./NODE_* -type d -empty -delete

clean-all: clean
	rm -rf ./NODE_* ./B0OTSTRAP ./node ./nameserver ./bootstrap

# Individual Run Shortcuts
run-bootstrap:
	@echo "Starting bootstrap"
	./bootstrap
	@echo "Bootstrap started"

run-nameserver:
	@echo "Starting nameserver"
	./nameserver
	@echo "Nameserver started"

APP := ./node
NODES := NODE_1 NODE_2 NODE_3 NODE_4
PID_DIR := .pids
run-nodes:
	@mkdir -p $(PID_DIR)
	@for i in $(NODES); do \
		echo "Starting $$i..."; \
		$(APP)  $$i & echo $$! > $(PID_DIR)/$$i.pid; \
	done
	@echo "All nodes started"

stop-nodes:
	@for f in $(PID_DIR)/*.pid; do \
		if [ -f $$f ]; then \
			PID=$$(cat $$f); \
			echo "Stopping $$f"; \
			kill -2 $$PID; \
			rm -f $$f; \
		fi; \
	done
	@rmdir $(PID_DIR) 2>/dev/null || true
	@echo "All nodes stopped"