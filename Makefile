MAKEFILE_DIR     := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
ORDER_SYSTEM_DIR := $(abspath $(MAKEFILE_DIR)../order-system)
OTEL_AGENT_VER   := 2.5.0
OTEL_AGENT_JAR   := $(MAKEFILE_DIR)otel/opentelemetry-javaagent.jar

# Docker Compose resolves ALL relative paths from the first -f file's directory.
# --project-directory overrides this: paths resolve from monitor-system/.
# ORDER_SYSTEM_DIR is exported so Compose can substitute it in override files
# (used to fix build.context for Java services).
export ORDER_SYSTEM_DIR

COMPOSE := docker compose \
	--project-directory $(MAKEFILE_DIR) \
	-f $(ORDER_SYSTEM_DIR)/docker-compose.yml \
	-f $(MAKEFILE_DIR)docker-compose.order-system.yml \
	-f $(MAKEFILE_DIR)docker-compose.yml

.PHONY: agent up down restart logs ps load anomaly-latency help

## agent: скачать OTel Java agent в otel/
agent:
	@mkdir -p $(MAKEFILE_DIR)otel
	@echo "→ Downloading opentelemetry-javaagent v$(OTEL_AGENT_VER)..."
	curl -sSL -o $(OTEL_AGENT_JAR) \
		"https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v$(OTEL_AGENT_VER)/opentelemetry-javaagent.jar"
	@echo "✓ Saved: $(OTEL_AGENT_JAR)"

## up: поднять весь стек (order-system + observability)
up:
	@if [ ! -f "$(OTEL_AGENT_JAR)" ]; then \
		echo "✗ OTel agent не найден. Сначала запустите: make agent"; \
		exit 1; \
	fi
	$(COMPOSE) up -d --build
	@echo ""
	@echo "✓ Стек запущен."
	@echo "  Grafana   → http://localhost:3000  (admin/admin)"
	@echo "  Jaeger    → http://localhost:16686"
	@echo "  Prometheus→ http://localhost:9090"
	@echo "  Kafka UI  → http://localhost:8090"

## down: остановить и удалить все контейнеры и тома
down:
	$(COMPOSE) down -v

## restart: перезапустить только стек observability (без пересборки order-system)
restart:
	docker compose -f $(MAKEFILE_DIR)docker-compose.yml restart

## logs: tail логов стека observability
logs:
	docker compose -f $(MAKEFILE_DIR)docker-compose.yml logs -f

## ps: статус всех контейнеров проекта
ps:
	$(COMPOSE) ps

## load: генерация 20 заказов через REST API order-service
load:
	@echo "→ Создаём 20 тестовых заказов..."
	@for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do \
		RESULT=$$(curl -sf -X POST http://localhost:8081/orders \
			-H "Content-Type: application/json" \
			-d "{\"userId\":\"load-user-$$i\",\"amount\":$$((i * 50))}" 2>&1); \
		if [ $$? -eq 0 ]; then \
			echo "  ✓ Order $$i created"; \
		else \
			echo "  ✗ Order $$i failed (order-service недоступен?)"; \
		fi; \
		sleep 0.3; \
	done
	@echo "✓ Нагрузка завершена. Обновите дашборд в Grafana."

## anomaly-latency: демо-сценарий burst 100 заказов для наблюдения p99 всплеска
anomaly-latency:
	@echo "→ Запускаем burst 100 заказов для демонстрации p99 всплеска..."
	@for i in $$(seq 1 100); do \
		curl -sf -X POST http://localhost:8081/orders \
			-H "Content-Type: application/json" \
			-d "{\"userId\":\"anomaly-$$i\",\"amount\":999}" > /dev/null 2>&1 & \
	done; wait
	@echo "✓ Burst завершён. Следите за p99 latency и consumer lag в Grafana."

## help: показать доступные команды
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## //'
