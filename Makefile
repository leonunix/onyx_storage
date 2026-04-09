SHELL := /bin/bash
.DEFAULT_GOAL := all

.PHONY: all engine-build engine-test engine-release dashboard-backend dashboard-frontend dashboard-build dashboard-backend-build dashboard-frontend-build

all: engine-build

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
