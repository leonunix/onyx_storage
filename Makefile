SHELL := /bin/bash
.DEFAULT_GOAL := all

.PHONY: all dev engine-build engine-test engine-release dashboard-backend dashboard-frontend dashboard-build dashboard-backend-build dashboard-frontend-build dashboard-release

all: engine-build

dev:
	@echo "  engine   → cargo run (config/default.toml)"
	@echo "  backend  → :8080"
	@echo "  frontend → :5173 (hot reload)"
	@echo "  Ctrl+C to stop all"
	@echo ""
	@trap 'kill 0' EXIT; \
	cargo run -- -c config/default.toml start 2>&1 & \
	(cd dashboard/backend && go run ./cmd/dashboardd) 2>&1 & \
	(cd dashboard/frontend && npm run dev) 2>&1 & \
	wait

engine-build:
	cargo build

engine-test:
	cargo test

engine-release:
	cargo build --release

dashboard-backend:
	cd dashboard/backend && go run ./cmd/dashboardd

dashboard-frontend:
	cd dashboard/frontend && npm install && npm run dev

dashboard-backend-build:
	cd dashboard/backend && go build ./...

dashboard-frontend-build:
	cd dashboard/frontend && npm install && npm run build

dashboard-build: dashboard-backend-build dashboard-frontend-build

dashboard-release: dashboard-frontend-build
	rm -rf dashboard/backend/internal/ui/dist
	cp -r dashboard/frontend/dist dashboard/backend/internal/ui/dist
	cd dashboard/backend && CGO_ENABLED=1 go build -o ../../bin/dashboardd ./cmd/dashboardd
