export OPENMETADATA_INGESTION_IMAGE_VERSION := 1.9.5

.DEFAULT_GOAL := help

.PHONY: help
help:  ## shows the Makefile targets with information
ifeq ($(OS),Windows_NT)
	@powershell -command " \
	   $$targets = Get-Content $(MAKEFILE_LIST) | Select-String -Pattern '^[a-zA-Z_-]+:.*?## .*$$'; \
	   foreach ($$target in $$targets) { \
	   $$parts = $$target.ToString() -split '##'; \
	   $$label = '{0,-36}' -f ($$parts[0] -replace ':.*', ''); \
	   $$description = $$parts[1].Trim(); \
	   Write-Host \"$$label $$description\"; \
	   } \
	"
else
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) |  awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
endif

.PHONY: local-openmetadata-stack
local-openmetadata-stack:  ## first shutdown any existing and then starts a local openmetadata stack for testing
	docker compose -f ./local-openmetadata-stack/docker-compose.yml down -v && docker compose -f ./local-openmetadata-stack/docker-compose.yml up -d

.PHONY: update-ingestion-container
update-ingestion-container: ## shutdown any existing services and start the local Open Metadata stack with a fresh build of the ingestion service
	docker compose -f ./local-openmetadata-stack/docker-compose.yml down -v && \
	docker compose -f ./local-openmetadata-stack/docker-compose.yml up -d --build ingestion
