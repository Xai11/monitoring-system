# Monitor-System — Архитектура и логика взаимодействия

## Общая схема

```
┌─────────────────────────────── order-system ────────────────────────────────┐
│                                                                              │
│  [HTTP :8081]         [Kafka: orders]      [Kafka: payments]                 │
│  order-service  ────────────────────────► payment-service                   │
│                                │                │                            │
│                                │                └──────► shipping-service    │
│                                │                │             │              │
│                                └────────────────┴─────────────┴──► analytics │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
         │ OTel Java agent (bytecode instrumentation, zero-code)
         ▼ OTLP gRPC :4317
┌──────────────────────────────────────────────────────────────────────────────┐
│                           OTel Collector                                     │
│  traces pipeline  ──► tail_sampling ──► Jaeger + Kafka (otel.spans)         │
│  metrics pipeline ──► Prometheus exporter (:8889)                           │
│  logs pipeline    ──► Loki                                                   │
└──────────────────────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
      Jaeger              Prometheus             Loki
    (трейсы)             (метрики)            (журналы)
                              │
                         Alertmanager ◄─── monitoring-service
                                           (Kafka Streams p99)
         │                    │                    │
         └────────────────────┴────────────────────┘
                              │
                           Grafana
                      (EDA Overview dashboard)
```

---

## Уровень 0 — Целевое приложение (order-system)

### Сервисы и топики Kafka

Приложение не изменялось. Его бизнес-логика:

```
POST /orders (order-service :8081)
  │
  ├─ publish → topic: orders
  │
  ├── payment-service (groupId: payment-group)
  │     │ delay: 500ms (hardcoded AtomicLong, изменяется через /admin/delay)
  │     └─ publish → topic: payments
  │           │
  │           └── shipping-service (groupId: shipping-group)
  │                 └─ publish → topic: shipments
  │
  └── analytics-service (одновременно слушает все три топика)
        groupId: analytics-orders-group   → topic: orders
        groupId: analytics-payments-group → topic: payments
        groupId: analytics-shipments-group→ topic: shipments
```

**Важный факт:** `payment-service` намеренно спит 500ms при обработке каждого заказа — это и есть источник наблюдаемой аномалии. `shipping-service` аналогично ~200ms.

**Kafka-брокер:** `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` — все топики создаются явно (бизнес-топики через `@Bean NewTopic` в коде, наши топики через `kafka-setup`).

---

## Уровень 1 — Инжекция агента (zero-code инструментация)

### Как агент подключается без правки кода

Файл `docker-compose.order-system.yml` — это **overlay**, накладываемый поверх `order-system/docker-compose.yml` при запуске через `docker compose -f`. Он **не изменяет** файлы приложения, только добавляет к каждому Java-сервису:

```yaml
volumes:
  - ./otel/opentelemetry-javaagent.jar:/otel/opentelemetry-javaagent.jar:ro
environment:
  JAVA_TOOL_OPTIONS: "-javaagent:/otel/opentelemetry-javaagent.jar"
  OTEL_SERVICE_NAME: "order-service"            # имя сервиса в трейсах
  OTEL_EXPORTER_OTLP_ENDPOINT: "http://otel-collector:4317"
  OTEL_EXPORTER_OTLP_PROTOCOL: "grpc"
  OTEL_TRACES_SAMPLER: "parentbased_always_on"  # 100% sampling
  OTEL_INSTRUMENTATION_KAFKA_EXPERIMENTAL_SPAN_ATTRIBUTES: "true"
  OTEL_METRICS_EXPORTER: "otlp"
  OTEL_LOGS_EXPORTER: "otlp"
```

`JAVA_TOOL_OPTIONS` — стандартный механизм JVM: переменная применяется автоматически к любому `java` процессу в контейнере. JVM подгружает агент до старта приложения.

### Что делает агент

Агент (OpenTelemetry Java Agent v2.5.0) инструментирует на уровне байткода:

| Фреймворк | Что создаёт |
|-----------|-------------|
| Spring MVC (HTTP) | Спан `kind=SERVER` на каждый входящий HTTP-запрос |
| Kafka Producer | Спан `kind=PRODUCER` на каждый `KafkaTemplate.send()` + инжекция W3C `traceparent` в заголовки Kafka-сообщения |
| Kafka Consumer | Спан `kind=CONSUMER` на каждый `@KafkaListener` + извлечение `traceparent` → все спаны одного заказа получают одинаковый `traceId` |
| JVM | Метрики heap, GC, threads |
| Logback | Перехват журналов, добавление `trace_id`/`span_id` в каждую запись |

**Итог:** один `POST /orders` порождает единый трейс из 9 спанов, пронизывающий 4 сервиса через 3 Kafka-топика — без единой строки кода в приложении.

---

## Уровень 2 — OTel Collector (центральный маршрутизатор телеметрии)

Collector запускается на портах `4317` (gRPC) и `4318` (HTTP). Все 4 Java-сервиса отправляют телеметрию сюда по OTLP.

### Три независимых pipeline

#### Pipeline: Traces (трейсы)

```
Receiver: otlp (gRPC :4317 / HTTP :4318)
    │
    ├─ processor: memory_limiter    → защита от OOM (лимит 400 МБ)
    ├─ processor: resource          → добавляет label: deployment.environment=local
    ├─ processor: tail_sampling     → буферизует спаны 10 секунд (decision_wait),
    │                                  затем принимает решение по всему трейсу целиком
    │                                  Политика: always_sample (100%)
    └─ processor: batch             → группирует в пакеты (timeout 1s, max 1024 спана)
         │
         ├─► Exporter: otlp/jaeger  → Jaeger All-in-One :4317 (хранение и UI)
         └─► Exporter: kafka/traces → Kafka topic: otel.spans
                                       encoding: otlp_json
                                       3 партиции, retry + queue
```

**Почему tail_sampling перед batch:** tail_sampling нужно видеть все спаны трейса до решения. Если batch стоит первым, он может отправить часть спанов до того, как tail_sampling их обработает — решение будет неполным.

#### Pipeline: Metrics (метрики)

```
Receiver: otlp
    │
    ├─ processor: memory_limiter
    ├─ processor: resource          → добавляет deployment.environment=local
    └─ processor: batch
         │
         └─► Exporter: prometheus  → HTTP :8889
                                     namespace: otel
                                     resource_to_telemetry_conversion: true
                                       (resource attrs → metric labels: service_name, etc.)
                                     enable_open_metrics: true
                                       (включает exemplars для связи метрика → трейс)
```

Prometheus scrape-ит этот endpoint каждые 15 секунд. Метрики приходят с префиксом `otel_` и лейблом `service_name`.

#### Pipeline: Logs (журналы)

```
Receiver: otlp
    │
    ├─ processor: memory_limiter
    ├─ processor: resource
    ├─ processor: resource/loki     → добавляет loki.resource.labels:
    │                                   service.name, service.version, deployment.environment
    └─ processor: batch
         │
         └─► Exporter: loki        → http://loki:3100/loki/api/v1/push
```

Loki индексирует журналы по лейблам `service_name`, `level`, `deployment_environment`. Каждая запись содержит поле `trace_id` (добавляется агентом через Logback MDC).

---

## Уровень 3 — Хранение и визуализация

### Jaeger (трейсы)

- **Хранилище:** in-memory (до 50 000 трейсов)
- **UI:** `http://localhost:16686`
- **Принимает:** OTLP gRPC от Collector
- **Использование:** поиск трейсов по service, operation, tag; waterfall-диаграмма спанов; связь через exemplars из Grafana

### Prometheus (метрики)

Prometheus scrape-ит три источника:

| Job | Endpoint | Метрики |
|-----|----------|---------|
| `otel-collector` | `otel-collector:8889` | `otel_http_server_*`, `otel_jvm_*`, `otel_messaging_*` — из Java-агента |
| `kafka-exporter` | `kafka-exporter:9308` | `kafka_consumergroup_lag`, `kafka_topic_partition_*` — из Kafka-брокера |
| `order-service`, `payment-service`, etc. | `:808x/actuator/prometheus` | Spring Boot Actuator: `http_server_requests_*`, JVM, DB pool |

**Retention:** 7 дней. **Exemplar storage** включён — хранит `trace_id` вместе с точкой метрики.

### Prometheus Alert Rules

Два независимых механизма алертинга работают параллельно:

**Механизм A — Prometheus rules** (файл `prometheus/alert-rules.yml`, интервал 30с):

| Алерт | Условие | Severity |
|-------|---------|----------|
| `ConsumerLagHigh` | `kafka_consumergroup_lag > 1000` в течение 2 минут | warning |
| `P99LatencyBreach` | p99 HTTP latency > 200ms в течение 2 минут | warning |
| `DlqRateHigh` | Скорость DLQ-топиков > 0.01/s в течение 2 минут | critical |
| `ServiceDown` | `up == 0` для Java-сервисов в течение 1 минуты | critical |

Prometheus → Alertmanager через стандартный push протокол.

### Loki (журналы)

- **Хранилище:** локальный TSDB, retention 7 дней
- **Индекс:** по лейблам (`service_name`, `level`, `deployment_environment`)
- **UI:** через Grafana Explore с LogQL
- **Связь:** поле `trace_id` в каждой записи → Derived Field в Grafana → переход в Jaeger

### Grafana (дашборды)

Все источники данных и дашборды провизионированы автоматически (из файлов в `grafana/provisioning/`):

**Datasources** (`grafana/provisioning/datasources/all.yml`):
- **Prometheus** — с `exemplarTraceIdDestinations` → Jaeger (связь ромбик на графике → трейс)
- **Jaeger** — с `tracesToLogsV2` → Loki (переход из спана → журналы этого сервиса)
- **Loki** — с `derivedFields` по полю `trace_id` → Jaeger (журнал → трейс)

**Dashboard** `EDA Overview` (`grafana/provisioning/dashboards/eda-overview.json`):
- p99 latency по сервисам (с exemplars)
- Consumer lag по топикам и группам
- DLQ rate
- Статус сервисов (up/down)
- Error rate

---

## Уровень 4 — Обнаружение аномалий (monitoring-service)

`monitoring-service` — отдельный Spring Boot сервис с Kafka Streams. Читает сырые трейсы из `otel.spans` и обнаруживает аномалии в реальном времени, не полагаясь на Prometheus scrape.

### Подготовка топиков (kafka-setup)

Перед стартом `monitoring-service` сервис `kafka-setup` (one-shot контейнер) создаёт два топика:

```
otel.spans          — 3 партиции, replFactor=1  (входящие трейсы из Collector)
otel.alerts.pending — 1 партиция, replFactor=1  (исходящие события алертов)
```

### Топология Kafka Streams (LatencyTopology — THRESHOLD rule)

```
Source: otel.spans (OTLP JSON batches)
    │
    ├─ flatMapValues → OtlpSpanParser.parse()
    │     Парсит JSON, фильтрует:
    │       kind=2 (SERVER, HTTP-запросы)  ← POST /orders
    │       kind=5 (CONSUMER, Kafka)       ← orders process, payments process
    │     Пропускает:  /actuator, health
    │     Возвращает:  SpanRecord(serviceName, durationMs, traceId)
    │
    ├─ filter → durationMs > 0
    │
    ├─ selectKey → key = serviceName
    │
    ├─ groupByKey (Grouped by serviceName)
    │
    ├─ windowedBy → TimeWindows
    │     windowSize = 60 секунд
    │     advanceBy  = 10 секунд  (hopping window)
    │     grace      = 10 секунд  (опоздавшие события)
    │
    ├─ aggregate → TDigestAgg
    │     TDigest(compression=100): структура данных для
    │     точного вычисления квантилей на потоке без хранения всех точек
    │     Для каждого окна хранит:
    │       – T-Digest: сжатое распределение длительностей
    │       – lastTraceId: для exemplar в алерте
    │
    ├─ filter → size >= 10 (minSampleCount)
    │           p99 > 200ms (p99ThresholdMs)
    │
    ├─ map → AlertEvent { service, ruleType=THRESHOLD, p99, threshold,
    │                      severity=warning, summary, traceId, firedAt }
    │     Side effect: alertmanager.fire(event)  → POST /api/v2/alerts
    │
    └─ to: otel.alerts.pending (ключ = serviceName, значение = JSON AlertEvent)
```

**Почему T-Digest, а не среднее:** T-Digest точно вычисляет хвостовые квантили (p99) даже на непрерывном потоке без буферизации всех точек. Для обнаружения латентных аномалий хвостовые квантили важнее среднего.

### Scheduled rules (ConsumerLagMonitor — RATE rule)

Параллельно с Kafka Streams, по расписанию (каждые `slide-seconds=10s`):

```
checkConsumerLag():
  Запрос к Prometheus: sum(kafka_consumergroup_lag)by(topic,consumergroup)
  Если lag > 1000 → AlertEvent(THRESHOLD) → Alertmanager + otel.alerts.pending

checkDlqRate():
  Запрос к Prometheus: rate(kafka_topic_partition_current_offset{topic=~".*dlq.*"}[2m])
  Если rate > 0.01/s → AlertEvent(RATE) → Alertmanager + otel.alerts.pending
```

### AlertSink (@KafkaListener)

Слушает `otel.alerts.pending`. Служит точкой доставки для алертов, опубликованных через топик (RATE/ABSENCE rules). Alertmanager дедуплицирует повторные вызовы.

---

## Уровень 5 — Алертинг (Alertmanager)

Alertmanager принимает алерты из **двух источников**:

| Источник | Механизм | Алерты |
|----------|---------- |--------|
| Prometheus | push при срабатывании rule | ConsumerLagHigh, P99LatencyBreach, DlqRateHigh, ServiceDown |
| monitoring-service | POST /api/v2/alerts | THRESHOLD (p99 Kafka Streams), RATE (DLQ) |

**Дедупликация:** одинаковые `labels` в течение `endsAt` (5 минут) считаются одним алертом.

**Маршрутизация** (`alertmanager/config.yml`):
- `severity=critical` → отдельный receiver (или Slack, если задан `SLACK_WEBHOOK_URL`)
- по умолчанию → default receiver (логирование)

---

## Сквозной путь одного заказа

Ниже — полный путь от HTTP-запроса до появления алерта:

```
1. curl POST http://localhost:8081/orders
       │
2. OTel agent создаёт спан kind=SERVER "POST /orders" в order-service
   traceId = abc123, spanId = span001
       │
3. order-service отправляет в Kafka topic: orders
   OTel agent создаёт спан kind=PRODUCER "orders publish"
   В заголовок Kafka-сообщения: traceparent: 00-abc123-span002-01
       │
4. payment-service получает сообщение из topic: orders
   OTel agent извлекает traceparent, создаёт спан kind=CONSUMER "orders process"
   traceId = abc123 (тот же!) — вот как трейс пересекает Kafka
   payment-service обрабатывает 500ms, публикует в topic: payments
       │
5. shipping-service получает из payments → обрабатывает 200ms → публикует в shipments
   analytics-service получает из orders, payments, shipments параллельно
       │
6. Все 9 спанов (traceId=abc123) отправляются в OTel Collector по OTLP gRPC
       │
7. Collector буферизует 10 секунд (tail_sampling decision_wait)
   Принимает решение: always_sample → все спаны проходят
       │
   ├─► Jaeger: трейс abc123 виден в UI, waterfall из 9 спанов
   └─► Kafka topic otel.spans: OTLP JSON batch со всеми 9 спанами
       │
8. monitoring-service Kafka Streams читает otel.spans
   OtlpSpanParser парсит batch, извлекает:
     SpanRecord("payment-service", 502.5ms, "abc123")
     SpanRecord("shipping-service", 202.1ms, "abc123")
   Добавляет в T-Digest скользящего окна (60s/10s) для каждого сервиса
       │
9. Через ≤10 секунд (slide) окно вычисляет p99:
   payment-service: p99=504ms > 200ms, n=21 >= 10 → ALERT
   shipping-service: p99=203ms > 200ms, n=21 >= 10 → ALERT
       │
10. AlertmanagerClient POST /api/v2/alerts (status 200 OK)
    AlertEvent → otel.alerts.pending
       │
11. Alertmanager показывает 2 активных алерта (TTL 5 минут)
    При настроенном SLACK_WEBHOOK_URL → уведомление в Slack
       │
12. Grafana EDA Overview:
    - p99 latency panel: всплеск до 500ms на payment-service
    - Exemplar (ромбик) на графике → клик → Jaeger трейс abc123
    - Loki: журналы payment-service с полем trace_id=abc123 → клик → Jaeger
```

---

## Связи между компонентами (матрица)

| От \ К | Jaeger | Prometheus | Loki | Alertmanager | Grafana |
|--------|--------|------------|------|--------------|---------|
| OTel Collector | OTLP gRPC | scrape :8889 | HTTP push | — | — |
| Prometheus | — | — | — | push rules | scrape |
| monitoring-service | — | HTTP query | — | POST /api/v2 | — |
| kafka-exporter | — | scrape :9308 | — | — | — |
| Grafana | HTTP query | HTTP query | HTTP query | — | — |
| Alertmanager | — | — | — | — | — |

---

## Конфигурация без перекомпиляции

Параметры обнаружения аномалий задаются в `monitoring-service/src/main/resources/application.yml` и переопределяются через переменные окружения:

```yaml
monitoring:
  latency:
    window-size-seconds: 60   # размер скользящего окна
    slide-seconds: 10         # шаг пересчёта
    p99-threshold-ms: 200     # порог алерта
    min-sample-count: 10      # минимум образцов (отсев ложных срабатываний)
  dlq:
    window-size-minutes: 5
    rate-threshold: 0.01      # сообщений/секунду
  consumer-lag:
    threshold-messages: 1000  # сообщений в очереди
```

Добавление нового типа правила: реализовать интерфейс `RuleHandler` → `@Scheduled` метод → публикация в `otel.alerts.pending`. Код детектора не меняется.
