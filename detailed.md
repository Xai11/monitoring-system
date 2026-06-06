# detailed.md — Полный контекст проекта monitor-system для Claude

Этот файл предназначен для ИИ-ассистента, который не имеет истории работы с этим проектом.
Прочитай его целиком перед любым действием. Он описывает что, зачем и как устроена каждая часть системы.

---

## 1. Что это за проект и зачем он существует

`monitor-system` — система наблюдаемости (observability), которая **подключается снаружи** к уже существующему Java-приложению `order-system`, не изменяя ни строки его кода.

**Задача:** дать инженеру возможность видеть:
- Как долго обрабатываются запросы и Kafka-сообщения в каждом сервисе (трейсы, p99 latency).
- Отстаёт ли Kafka-консьюмер от потока сообщений (consumer lag).
- Есть ли ошибки, падения сервисов, аномальные задержки (алерты).
- Какие журналы соответствуют конкретному запросу (trace_id → логи).

**Ключевое ограничение:** файлы в `/project/order-system/` НЕЛЬЗЯ трогать. Вся наблюдаемость подключается через:
1. OTel Java-агент, инжектируемый через переменные окружения Docker.
2. Docker Compose override-файл (`docker-compose.order-system.yml`), который лежит в нашем каталоге.

Если задача кажется невыполнимой без правки `order-system` — скажи об этом и предложи неинвазивную альтернативу.

---

## 2. Расположение файлов проекта

Весь наш код находится в `/home/alex/projects/monitor-system/`. Структура:

```
monitor-system/
├── CLAUDE.md                            ← правила для Claude (прочитай первым)
├── detailed.md                          ← этот файл
├── architecture.md                      ← краткая схема взаимодействия
├── help.md                              ← инструкция по запуску и наблюдению
├── Makefile                             ← все команды (make up, make load, ...)
│
├── docker-compose.yml                   ← стек наблюдаемости (наши сервисы)
├── docker-compose.order-system.yml      ← overlay: инжектирует OTel-агент в order-system
│
├── otel/
│   ├── collector-config.yaml            ← конфиг OTel Collector (receivers/processors/exporters)
│   └── opentelemetry-javaagent.jar      ← агент v2.5.0 (~21 МБ, в git не коммитится)
│
├── prometheus/
│   ├── prometheus.yml                   ← scrape_configs + alertmanager endpoint
│   └── alert-rules.yml                  ← 4 правила алертинга (Prometheus side)
│
├── alertmanager/
│   └── config.yml                       ← маршрутизация алертов, Slack (опц.)
│
├── loki/
│   └── config.yaml                      ← хранилище журналов, retention 7 дней
│
├── grafana/
│   └── provisioning/
│       ├── datasources/all.yml          ← Prometheus + Jaeger + Loki с exemplars/derived fields
│       └── dashboards/
│           ├── dashboards.yml           ← автозагрузка дашбордов
│           └── eda-overview.json        ← главный дашборд (11 панелей)
│
└── monitoring-service/                  ← наш Java-сервис (Kafka Streams + Spring Boot)
    ├── Dockerfile                       ← multi-stage: maven:3.9 → eclipse-temurin:17-jre
    ├── pom.xml                          ← Spring Boot 3.2.5, spring-kafka, kafka-streams, t-digest:3.3
    └── src/main/
        ├── resources/application.yml   ← порт 8084, Kafka, пороги алертов
        └── java/com/example/monitoring/
            ├── MonitoringApplication.java          ← @SpringBootApplication, @EnableKafkaStreams
            ├── config/MonitoringProperties.java    ← @ConfigurationProperties("monitoring")
            ├── rule/RuleHandler.java               ← интерфейс: getRuleType()
            ├── alerting/
            │   ├── AlertEvent.java                 ← record: данные одного алерта
            │   ├── AlertmanagerClient.java          ← POST /api/v2/alerts в Alertmanager
            │   └── AlertSink.java                  ← @KafkaListener на otel.alerts.pending
            ├── streams/
            │   ├── SpanRecord.java                 ← record: serviceName, durationMs, traceId
            │   ├── TDigestAgg.java                 ← обёртка T-Digest для оконной агрегации
            │   ├── TDigestAggSerde.java             ← Kafka Serde для TDigestAgg
            │   ├── OtlpSpanParser.java              ← парсер OTLP JSON → List<SpanRecord>
            │   └── LatencyTopology.java             ← @Bean KStream: главная топология Kafka Streams
            └── lag/
                └── ConsumerLagMonitor.java          ← @Scheduled: consumer lag + DLQ через Prometheus API
```

---

## 3. Целевое приложение order-system (не трогаем, только читаем)

Расположение: `/home/alex/projects/order-system/`

Это событийно-ориентированный сервис заказов на Spring Boot 3 + Apache Kafka.

### Сервисы и их роли

| Сервис | Порт | Что делает |
|--------|------|-----------|
| `order-service` | 8081 | Принимает HTTP POST /orders, публикует событие в Kafka |
| `payment-service` | 8082 | Обрабатывает заказы из Kafka, симулирует оплату (500ms задержка!), публикует результат |
| `shipping-service` | 8083 | Обрабатывает результаты оплаты, создаёт отгрузку (~200ms), публикует в `shipments` |
| `analytics-service` | 8084 | Читает все три топика параллельно, сохраняет в PostgreSQL |

### Kafka-топики (в order-system)

| Топик | Producer | Consumer group(s) |
|-------|----------|------------------|
| `orders` | order-service | `payment-group`, `analytics-orders-group` |
| `payments` | payment-service | `shipping-group`, `analytics-payments-group` |
| `shipments` | shipping-service | `analytics-shipments-group` |

**Важно:** `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` — автосоздание топиков отключено. Бизнес-топики создаются через `@Bean NewTopic` в коде сервисов. Наши топики (`otel.spans`, `otel.alerts.pending`) создаёт one-shot контейнер `kafka-setup`.

### Намеренная задержка в payment-service

Класс `DelayConfig` содержит `AtomicLong delayMs = new AtomicLong(500)`. При каждой обработке заказа `payment-service` спит 500 мс. Это намеренная "медленная" операция, которую система мониторинга должна обнаруживать. Значение можно изменить через REST: `POST /admin/delay?ms=100`.

### Инфраструктура order-system

- **Kafka:** `apache/kafka:3.7.0`, KRaft mode (без ZooKeeper), порт `9092` (internal), `9094` (external).
- **PostgreSQL:** `postgres:15-alpine`, база `analytics`, порт `5432`.
- **Kafka UI:** `provectuslabs/kafka-ui`, порт `8090`.
- **Docker-сеть:** создаётся Docker Compose автоматически. Наши сервисы подключаются к ней через `external network`.

---

## 4. Механизм неинвазивного подключения (Zero-code instrumentation)

### Как работает Docker Compose merge

Команда запуска (из Makefile):
```
docker compose \
  --project-directory /home/alex/projects/monitor-system \
  -f /home/alex/projects/order-system/docker-compose.yml \
  -f /home/alex/projects/monitor-system/docker-compose.order-system.yml \
  -f /home/alex/projects/monitor-system/docker-compose.yml \
  up -d
```

Docker Compose **сливает** три файла в памяти. Результат — один объединённый стек. Файлы на диске не изменяются. `--project-directory` задаёт базовый каталог для разрешения относительных путей (`./otel/...`).

**Почему нужен `--project-directory`:** без него относительные пути в overlay-файле резолвились бы от первого `-f` файла (т.е. от `order-system/`), и `./otel/opentelemetry-javaagent.jar` искался бы в `/home/alex/projects/order-system/otel/` — которого не существует.

**Почему `ORDER_SYSTEM_DIR` экспортируется:** потому что `build.context` в `docker-compose.order-system.yml` задан как `${ORDER_SYSTEM_DIR}`. Без экспорта переменная недоступна внутри Compose-файла. Оригинальный `context: .` из `order-system/docker-compose.yml` при `--project-directory monitor-system` указывал бы на наш каталог — тоже неправильно.

### Что добавляет docker-compose.order-system.yml

Для каждого из 4 Java-сервисов (`order-service`, `payment-service`, `shipping-service`, `analytics-service`) добавляет:

```yaml
volumes:
  - ./otel/opentelemetry-javaagent.jar:/otel/opentelemetry-javaagent.jar:ro
environment:
  JAVA_TOOL_OPTIONS: "-javaagent:/otel/opentelemetry-javaagent.jar"
  OTEL_SERVICE_NAME: "order-service"               # уникально для каждого
  OTEL_EXPORTER_OTLP_ENDPOINT: "http://otel-collector:4317"
  OTEL_EXPORTER_OTLP_PROTOCOL: "grpc"
  OTEL_TRACES_SAMPLER: "parentbased_always_on"     # 100% sampling
  OTEL_INSTRUMENTATION_KAFKA_EXPERIMENTAL_SPAN_ATTRIBUTES: "true"
  OTEL_METRICS_EXPORTER: "otlp"
  OTEL_LOGS_EXPORTER: "otlp"
  OTEL_RESOURCE_ATTRIBUTES: "deployment.environment=local,service.version=1.0.0"
```

`JAVA_TOOL_OPTIONS` — стандартный механизм JVM: применяется ко всем `java`-процессам в контейнере до загрузки приложения. JVM инициализирует агент, который патчит байткод при загрузке классов.

### Что инструментирует OTel Java Agent v2.5.0

Агент автоматически, без изменения кода, создаёт:

| Фреймворк/библиотека | Тип спана | Что именно |
|---------------------|-----------|-----------|
| Spring MVC (входящие HTTP) | `kind=SERVER (2)` | Каждый `@RequestMapping` — один спан |
| Apache Kafka Producer | `kind=PRODUCER (3)` | Каждый `KafkaTemplate.send()` + инжекция W3C `traceparent` в заголовки сообщения |
| Apache Kafka Consumer | `kind=CONSUMER (5)` | Каждый `@KafkaListener` + извлечение `traceparent` из заголовков → продолжение трейса |
| JDBC (PostgreSQL) | `kind=CLIENT (3)` | Каждый SQL-запрос |
| JVM метрики | — | heap, GC, threads через OTLP |
| Logback | — | Перехват журналов, добавление `trace_id`/`span_id` в MDC каждой записи |

**Как трейс пересекает Kafka:** producer вставляет `traceparent: 00-{traceId}-{spanId}-01` в заголовок Kafka-сообщения. Consumer читает этот заголовок и создаёт дочерний спан с тем же `traceId`. Поэтому один заказ порождает единый трейс из 9 спанов, объединяющий 4 сервиса.

### Пример одного трейса (9 спанов, traceId=abc123)

```
order-service    POST /orders          kind=SERVER   ~10ms   (root span)
order-service    orders publish        kind=PRODUCER ~15ms   (child)
payment-service  orders process        kind=CONSUMER ~502ms  (child, 500ms delay!)
payment-service  payments publish      kind=PRODUCER ~2ms    (child)
shipping-service payments process      kind=CONSUMER ~202ms  (child)
shipping-service shipments publish     kind=PRODUCER ~2ms    (child)
analytics-service orders process       kind=CONSUMER ~4ms    (parallel)
analytics-service payments process     kind=CONSUMER ~2ms    (parallel)
analytics-service shipments process    kind=CONSUMER ~1ms    (parallel)
```

---

## 5. OTel Collector — центральный маршрутизатор телеметрии

**Образ:** `otel/opentelemetry-collector-contrib:0.102.0`
**Конфиг:** `otel/collector-config.yaml`
**Порты:** `4317` (OTLP gRPC приём), `4318` (OTLP HTTP приём), `8889` (Prometheus scrape endpoint)

Collector получает телеметрию от всех 4 Java-сервисов и маршрутизирует по трём независимым pipeline.

### Pipeline: Traces

```
receivers:  [otlp]
processors: [memory_limiter → resource → tail_sampling → batch]
exporters:  [otlp/jaeger, kafka/traces]
```

**memory_limiter:** предотвращает OOM — ограничивает использование памяти 400 МБ (spike 100 МБ).

**resource:** добавляет атрибут `deployment.environment=local` ко всем спанам.

**tail_sampling:** ключевой процессор. Буферизует все спаны в течение `decision_wait: 10s`, затем принимает решение по **всему трейсу целиком**. Это в отличие от head sampling, где решение принимается при первом спане. Текущая политика: `always_sample` (100%). **Порядок критичен:** tail_sampling ДОЛЖЕН стоять перед batch. Если batch стоит первым — он отправит часть спанов до того, как tail_sampling соберёт полный трейс.

**batch:** группирует принятые спаны в пакеты (timeout 1s, max 1024) для эффективной отправки.

**otlp/jaeger:** отправляет в Jaeger All-in-One на `jaeger:4317` (OTLP gRPC, TLS отключён).

**kafka/traces:** отправляет в Kafka топик `otel.spans` в формате `otlp_json`. Конфиг:
```yaml
brokers: ["kafka:9092"]
topic: otel.spans
encoding: otlp_json        # формат: OTLP protobuf, сериализованный в JSON
timeout: 10s
retry_on_failure: {enabled: true, initial_interval: 5s, max_interval: 60s}
sending_queue: {enabled: true, queue_size: 1000}
```

### Pipeline: Metrics

```
receivers:  [otlp]
processors: [memory_limiter → resource → batch]
exporters:  [prometheus]
```

**prometheus exporter:**
```yaml
endpoint: "0.0.0.0:8889"
namespace: otel              # все метрики от агента получают префикс otel_
enable_open_metrics: true    # включает exemplars в формате OpenMetrics
resource_to_telemetry_conversion: {enabled: true}  # resource attrs → labels (service_name и др.)
```

Prometheus scrape-ит этот endpoint. Метрики от OTel-агента имеют лейбл `service_name`, а не `job`.

### Pipeline: Logs

```
receivers:  [otlp]
processors: [memory_limiter → resource → resource/loki → batch]
exporters:  [loki]
```

**resource/loki:** добавляет `loki.resource.labels: "service.name,service.version,deployment.environment"` — эти resource-атрибуты станут индексируемыми лейблами в Loki.

**loki exporter:** HTTP push на `http://loki:3100/loki/api/v1/push`.

---

## 6. Хранилища данных

### Jaeger (трейсы)

**Образ:** `jaegertracing/all-in-one:1.57`
**Порты:** `16686` (UI), `4317-4318` (OTLP приём, только internal)
**Хранилище:** in-memory, до 50 000 трейсов (нет персистентности после рестарта)
**Переменные:** `COLLECTOR_OTLP_ENABLED: "true"`, `SPAN_STORAGE_TYPE: "memory"`

В UI (`http://localhost:16686`) можно искать трейсы по сервису, операции, тегу, временному диапазону.

### Prometheus (метрики)

**Образ:** `prom/prometheus:v2.52.0`
**Порт:** `9090`
**Конфиг:** `prometheus/prometheus.yml`
**Персистентность:** volume `prometheus_data`, retention 7 дней

Prometheus scrape-ит **6 источников**:

| Job | Target | Что за метрики |
|-----|--------|---------------|
| `otel-collector` | `otel-collector:8889` | OTel-агент: `otel_http_server_request_duration_seconds_*`, `otel_jvm_*`, `otel_messaging_*`. Лейбл: `service_name` |
| `order-service` | `order-service:8081/actuator/prometheus` | Spring Actuator: `http_server_requests_seconds_*`, JVM. Лейбл: `job` |
| `payment-service` | `payment-service:8082/actuator/prometheus` | Аналогично |
| `shipping-service` | `shipping-service:8083/actuator/prometheus` | Аналогично |
| `analytics-service` | `analytics-service:8084/actuator/prometheus` | Аналогично |
| `kafka-exporter` | `kafka-exporter:9308` | `kafka_consumergroup_lag`, `kafka_topic_partition_current_offset`, `kafka_topic_partitions` |

**Разница OTel vs Actuator метрик:**
- OTel-агент экспортирует гистограммы с bucket-ами — нужны для `histogram_quantile(0.99, ...)`.
- Spring Actuator экспортирует summary (count/sum) — для простых rate и up/down метрик.
- Для p99 в Grafana dashboard используем OTel метрики (prefix `otel_`).

**Exemplar storage включён:** флаг `--enable-feature=exemplar-storage`. Prometheus сохраняет `trace_id` рядом с каждой точкой метрики. В Grafana это отображается как кликабельные ромбики на графиках.

### Prometheus Alert Rules (файл `prometheus/alert-rules.yml`)

Два источника алертинга работают параллельно и независимо:

**Источник A — Prometheus rules** (этот файл):

```yaml
ConsumerLagHigh:
  expr: kafka_consumergroup_lag > 1000
  for: 2m
  severity: warning

P99LatencyBreach:
  expr: histogram_quantile(0.99,
          sum(rate(http_server_requests_seconds_bucket{uri!~"/actuator.*"}[5m]))
          by (le, job)) > 0.2
  for: 2m
  severity: warning

DlqRateHigh:
  expr: sum(rate(kafka_topic_partition_current_offset{topic=~".*[.-]dlq$|.*dead[.-]letter.*"}[5m]))
          by (topic) > 0.01
  for: 2m
  severity: critical

ServiceDown:
  expr: up{job=~"order-service|payment-service|..."} == 0
  for: 1m
  severity: critical
```

**Источник B — monitoring-service** (Kafka Streams, описан ниже): напрямую вызывает Alertmanager API при срабатывании THRESHOLD rule.

### Alertmanager

**Образ:** `prom/alertmanager:v0.27.0`
**Порт:** `9093`
**API версия:** используется `/api/v2/alerts` (v1 удалён в 0.28.0 — важно!)

**Маршрутизация** (`alertmanager/config.yml`):
- `severity=critical` → group_wait 5s, repeat 1h
- `severity=warning` → стандартный receiver
- `inhibit_rules`: критический алерт подавляет одноимённый warning

**Slack:** закомментирован, активируется через переменную `SLACK_WEBHOOK_URL`.

**Дедупликация:** если алерт с теми же `labels` поступает повторно до истечения `endsAt` — Alertmanager не создаёт дубль. Наш monitoring-service выставляет `endsAt = firedAt + 5 минут`.

### Loki (журналы)

**Образ:** `grafana/loki:2.9.8`
**Порт:** `3100`
**Конфиг:** `loki/config.yaml`
**Хранилище:** TSDB (filesystem), volume `loki_data`, retention 168h (7 дней)
**Схема:** v13 (актуальная)

Loki индексирует только лейблы, но не содержимое строк журнала (полнотекстовый поиск через `|=` в LogQL по нескольким шагам постобработки). Лейблы: `service_name`, `service_version`, `deployment_environment`, `level`, `job`.

### Grafana

**Образ:** `grafana/grafana:10.4.3`
**Порт:** `3000`
**Логин:** admin / admin
**Персистентность:** volume `grafana_data`

Все настройки провизионированы декларативно из файлов — UI-настройка не требуется.

**Datasources** (`grafana/provisioning/datasources/all.yml`) — три источника со связями:

| Datasource | UID | Связь |
|-----------|-----|-------|
| Prometheus | `prometheus` | exemplarTraceIdDestinations → Jaeger: клик на ромбик открывает трейс |
| Jaeger | `jaeger` | tracesToLogsV2 → Loki: из спана открываются логи сервиса за период спана |
| Loki | `loki` | derivedFields: поле `trace_id` → Jaeger: из строки лога открывается трейс |

**Dashboard** `EDA Overview` (uid=`eda-overview`, 11 панелей). Ключевые запросы:
- p99: `histogram_quantile(0.99, sum(rate(otel_http_server_request_duration_seconds_bucket[2m])) by (le, service_name))`
- Consumer lag: `sum(kafka_consumergroup_lag) by (topic, consumergroup)`
- Service up: `up{job=~"order-service|payment-service|shipping-service|analytics-service"}`
- DLQ rate: `sum(rate(kafka_topic_partition_current_offset{topic=~".*dlq.*"}[5m])) by (topic)`

---

## 7. Kafka Exporter

**Образ:** `danielqsj/kafka-exporter:v1.7.0`
**Порт:** `9308`
**Подключение:** `--kafka.server=kafka:9092`

Экспортирует метрики Kafka-брокера в формате Prometheus. Ключевые метрики:

```
kafka_consumergroup_lag{topic, consumergroup, partition}
kafka_topic_partition_current_offset{topic, partition}
kafka_topic_partitions{topic}
```

**Важно:** имя метрики `kafka_consumergroup_lag` — без подчёркивания между `consumer` и `group`. Это частая ошибка: `kafka_consumer_group_lag` не существует.

---

## 8. monitoring-service — Детальное описание

**Технологии:** Spring Boot 3.2.5, Kafka Streams 3.6.2, T-Digest 3.3, Java 17
**Порт:** `8084` внутри контейнера, пробрасывается на `8085` хоста (8084 занят analytics-service снаружи)
**Конфиг:** `monitoring-service/src/main/resources/application.yml`

### Внешние зависимости

```xml
spring-kafka        → Kafka Streams + @KafkaListener + KafkaTemplate
kafka-streams       → KStream, TimeWindows, aggregate
t-digest:3.3        → com.tdunning.math.stats.TDigest, MergingDigest
jackson-datatype-jsr310  → сериализация java.time.Instant
```

### MonitoringProperties (`config/MonitoringProperties.java`)

`@ConfigurationProperties(prefix = "monitoring")` — маппит YAML в Java-объект.

Параметры с дефолтами (переопределяются через env или application.yml):

```yaml
monitoring:
  latency:
    window-size-seconds: 60     # размер скользящего окна T-Digest
    slide-seconds: 10           # шаг пересчёта (hopping window)
    p99-threshold-ms: 200       # порог p99 для алерта
    min-sample-count: 10        # минимум образцов — защита от ложных срабатываний при 1-2 спанах
  dlq:
    window-size-minutes: 5
    rate-threshold: 0.01        # сообщений/секунду в DLQ-топиках
  consumer-lag:
    threshold-messages: 1000    # сообщений в очереди
```

### RuleHandler (`rule/RuleHandler.java`)

Интерфейс расширения системы:
```java
public interface RuleHandler {
    String getRuleType(); // "THRESHOLD", "RATE", "ABSENCE"
}
```

Для добавления нового типа правила: реализовать `RuleHandler`, добавить `@Scheduled` метод или топологию Kafka Streams. Код детектора не меняется.

### AlertEvent (`alerting/AlertEvent.java`)

```java
public record AlertEvent(
    String service,     // имя сервиса (например, "payment-service")
    String ruleType,    // "THRESHOLD" | "RATE" | "ABSENCE"
    double value,       // фактическое значение (p99 в мс, lag в сообщениях, rate)
    double threshold,   // порог срабатывания
    String severity,    // "warning" | "critical"
    String summary,     // человекочитаемое описание
    String traceId,     // exemplar — traceId последнего спана в окне (может быть null)
    Instant firedAt     // момент срабатывания
) {}
```

### SpanRecord (`streams/SpanRecord.java`)

```java
public record SpanRecord(
    String serviceName,   // из resource attribute "service.name"
    double durationMs,    // (endTimeUnixNano - startTimeUnixNano) / 1_000_000
    String traceId        // из span.traceId
) {}
```

### OtlpSpanParser (`streams/OtlpSpanParser.java`)

Парсит OTLP JSON batch (формат Kafka exporter из OTel Collector). Структура входного JSON:
```json
{
  "resourceSpans": [{
    "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "payment-service"}}]},
    "scopeSpans": [{
      "spans": [{
        "traceId": "abc123...",
        "kind": 5,
        "name": "orders process",
        "startTimeUnixNano": "1717699200000000000",
        "endTimeUnixNano": "1717699200502000000"
      }]
    }]
  }]
}
```

Фильтры:
- `kind == 2` (SPAN_KIND_SERVER) — входящие HTTP-запросы
- `kind == 5` (SPAN_KIND_CONSUMER) — обработка Kafka-сообщений

Пропускает `kind=3` (PRODUCER — само по себе быстро, не интересно для latency), `kind=1` (INTERNAL). Также пропускает спаны с именем `/actuator` или `health`.

**Важно:** `startTimeUnixNano` и `endTimeUnixNano` — строки (не числа) в формате наносекунд Unix epoch. Нужен `Long.parseLong()`, не `asLong()` (Jackson асLong() обрезает при переполнении int).

### TDigestAgg (`streams/TDigestAgg.java`)

Mutable aggregate для Kafka Streams windowed computation. Внутри — `TDigest.createMergingDigest(100)`.

**Алгоритм T-Digest** (compression=100):
- Структура данных: дерево центроидов (кластеров точек).
- Входящая точка `x` добавляется к ближайшему центроиду, если это не нарушает ограничение размера (функция от compression и позиции в распределении).
- Хвостовые квантили (p99, p999) сохраняются точнее центральных — это свойство алгоритма.
- `compression=100` → точность p99 примерно ±1% при 100+ точках.
- `quantile(0.99)` — метод для получения p99.

Дополнительно хранит `lastTraceId` — `traceId` последней добавленной точки. Используется как exemplar в алерте (ссылка на конкретный медленный трейс).

### TDigestAggSerde (`streams/TDigestAggSerde.java`)

Kafka Serde для сериализации `TDigestAgg` в state store (changelog топик).

Wire format:
```
[4 байта: длина tdigest] [N байт: tdigest в small format] [4 байта: длина traceId] [M байт: traceId UTF-8]
```

Сериализация:
```java
ByteBuffer tBuf = ByteBuffer.allocate(digest.smallByteSize());
digest.asSmallBytes(tBuf);  // записывает в small format
tBuf.flip();
```

Десериализация:
```java
MergingDigest.fromBytes(ByteBuffer.wrap(tBytes));
```

**Важно:** в t-digest 3.3 нет метода `smallByteBuffer()` (он удалён). Правильный API: `smallByteSize()` + `asSmallBytes(ByteBuffer)`.

### LatencyTopology (`streams/LatencyTopology.java`)

Главная Kafka Streams топология. Регистрируется как `@Bean KStream`. Реализует `RuleHandler` с `getRuleType() = "THRESHOLD"`.

**Полная топология:**

```
builder.stream("otel.spans", Consumed.with(String, String))
  │
  ├─ flatMapValues(parser::parse)
  │    Один OTLP-batch → List<SpanRecord>
  │
  ├─ filter(span.durationMs > 0)
  │    Отбрасывает невалидные записи
  │
  ├─ selectKey(span.serviceName)
  │    Переключает ключ с null на имя сервиса
  │
  ├─ groupByKey(Grouped.with(String, JsonSerde<SpanRecord>))
  │    Группирует по сервису; создаёт repartition-топик:
  │    "monitoring-service-latency-tdigest-repartition"
  │
  ├─ windowedBy(TimeWindows.ofSizeAndGrace(60s, 10s).advanceBy(10s))
  │    Hopping window: размер 60s, шаг 10s, grace 10s
  │    Каждый момент времени → несколько перекрывающихся окон
  │
  ├─ aggregate(
  │    TDigestAgg::new,
  │    (svc, span, agg) -> agg.add(span.durationMs, span.traceId),
  │    Materialized.as("latency-tdigest").withValueSerde(TDigestAggSerde)
  │  )
  │    State store: "latency-tdigest" (RocksDB под капотом)
  │    Changelog: "monitoring-service-latency-tdigest-changelog"
  │
  ├─ toStream()
  │    TimeWindowedKTable → KStream (испускает обновлённые значения по мере поступления данных)
  │
  ├─ filter(agg.size >= 10 AND agg.p99 > 200)
  │    Двойное условие: достаточно данных И порог превышен
  │
  ├─ map((windowedKey, agg) -> {
  │    AlertEvent event = new AlertEvent(...)
  │    alertmanager.fire(event)           ← side effect!
  │    return KeyValue(serviceName, json)
  │  })
  │
  └─ to("otel.alerts.pending", Produced.with(String, String))
```

**Hopping window:** в отличие от tumbling window (непересекающиеся), hopping создаёт overlapping окна. С размером 60s и шагом 10s — каждая точка попадает в 6 окон. Это означает, что при `p99 > threshold` алерт может сработать до 6 раз в минуту (по одному на каждое активное окно). Alertmanager дедуплицирует их по labels.

**State store changelog:** Kafka Streams автоматически создаёт changelog-топик для персистентности state store. При `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` Kafka Streams всё равно создаёт его через AdminClient (это не то же самое, что авто-создание при публикации).

### AlertmanagerClient (`alerting/AlertmanagerClient.java`)

POST на `{alertmanagerUrl}/api/v2/alerts` (v2, не v1 — v1 удалён в Alertmanager 0.28.0).

Формат payload (массив алертов):
```json
[{
  "labels": {
    "alertname": "THRESHOLD_payment_service",
    "service": "payment-service",
    "severity": "warning",
    "ruletype": "THRESHOLD"
  },
  "annotations": {
    "summary": "p99=504ms > threshold=200ms (n=21)",
    "trace_id": "abc123def456..."
  },
  "startsAt": "2026-06-05T18:42:12Z",
  "endsAt": "2026-06-05T18:47:12Z"
}]
```

`endsAt = firedAt + 5 минут` — TTL алерта. Если в течение 5 минут алерт не поступит снова, Alertmanager его закроет.

### ConsumerLagMonitor (`lag/ConsumerLagMonitor.java`)

Реализует `RuleHandler` с `getRuleType() = "RATE"`. Два `@Scheduled` метода:

**checkConsumerLag()** (каждые `slide-seconds=10` секунд):
```
GET http://prometheus:9090/api/v1/query?query=sum(kafka_consumergroup_lag)by(topic,consumergroup)
Если value > 1000 → AlertEvent(THRESHOLD) → alertmanager.fire() + publish to otel.alerts.pending
```

**checkDlqRate()** (каждые `dlq.window-size-minutes=5` минут):
```
GET http://prometheus:9090/api/v1/query?query=
    sum(rate(kafka_topic_partition_current_offset{topic=~".*[.-]dlq$|.*dead[.-]letter.*"}[2m]))by(topic)
Если rate > 0.01 → AlertEvent(RATE) → alertmanager.fire() + publish to otel.alerts.pending
```

### AlertSink (`alerting/AlertSink.java`)

`@KafkaListener(topics = "otel.alerts.pending", groupId = "alertmanager-sink")` — читает JSON-события алертов из топика. Логирует их. Служит точкой доставки для RATE/ABSENCE правил, опубликованных в топик не через LatencyTopology. Alertmanager дедуплицирует повторные вызовы.

---

## 9. Инициализация Kafka-топиков (kafka-setup)

One-shot контейнер (`restart: "no"`, `image: apache/kafka:3.7.0`) создаёт наши топики перед стартом monitoring-service:

```bash
kafka-topics.sh --create --if-not-exists --topic otel.spans --partitions 3 --replication-factor 1
kafka-topics.sh --create --if-not-exists --topic otel.alerts.pending --partitions 1 --replication-factor 1
```

`depends_on: kafka (service_healthy)` — ждёт готовности брокера.
`monitoring-service` имеет `depends_on: kafka-setup (service_completed_successfully)` — стартует только после успешного завершения setup.

---

## 10. Docker Compose: зависимости и порядок запуска

```
kafka (healthy)
  ├── kafka-setup (completed_successfully)
  │      └── monitoring-service (healthy)
  ├── kafka-exporter
  ├── order-service, payment-service, shipping-service, analytics-service
  └── otel-collector (healthy)
         └── (получает телеметрию от Java-сервисов)

alertmanager (healthy) ─┐
prometheus (healthy) ───┤── monitoring-service depends on both
                        │
jaeger (healthy) ───────┐
prometheus (healthy) ───┤── grafana depends on all three
loki (healthy) ─────────┘
```

---

## 11. Карта портов

| Порт хоста | Контейнер | Сервис | Примечание |
|-----------|-----------|--------|-----------|
| 3000 | grafana:3000 | Grafana | UI, admin/admin |
| 9090 | prometheus:9090 | Prometheus | UI + API |
| 16686 | jaeger:16686 | Jaeger UI | Трейсы |
| 3100 | loki:3100 | Loki | HTTP API |
| 9308 | kafka-exporter:9308 | Kafka Exporter | Prometheus scrape |
| 9093 | alertmanager:9093 | Alertmanager | UI + API v2 |
| 4317 | otel-collector:4317 | OTel Collector | OTLP gRPC приём |
| 4318 | otel-collector:4318 | OTel Collector | OTLP HTTP приём |
| 8889 | otel-collector:8889 | OTel Collector | Prometheus scrape |
| **8085** | monitoring-service:8084 | monitoring-service | **8084 занят analytics-service!** |
| 8081 | order-service:8081 | order-service | REST API |
| 8082 | payment-service:8082 | payment-service | REST + /admin/delay |
| 8083 | shipping-service:8083 | shipping-service | REST |
| 8084 | analytics-service:8084 | analytics-service | REST |
| 9094 | kafka:9094 | Kafka | External listener |
| 8090 | kafka-ui:8080 | Kafka UI | Просмотр топиков |
| 5432 | postgres:5432 | PostgreSQL | analytics DB |

---

## 12. Makefile — команды

| Команда | Что делает |
|---------|-----------|
| `make agent` | Скачивает `opentelemetry-javaagent.jar` v2.5.0 в `otel/` (нужно один раз) |
| `make up` | Поднимает весь стек: `docker compose ... up -d --build`. Проверяет наличие jar. |
| `make down` | `docker compose ... down -v` — удаляет контейнеры и все volumes |
| `make restart` | Перезапускает только observability-стек (без order-system) |
| `make logs` | `docker compose logs -f` для observability-стека |
| `make ps` | Статус всех 15 контейнеров |
| `make load` | 20 заказов последовательно (интервал 0.3s) через `POST /orders` |
| `make anomaly-latency` | 100 параллельных заказов (burst) — провоцирует всплеск p99 и алерты |
| `make help` | Список команд |

---

## 13. Известные особенности и исправленные баги

### 1. TDigestAgg: нет метода `smallByteBuffer()`
T-Digest 3.3 не содержит `smallByteBuffer()`. Правильный API:
```java
ByteBuffer buf = ByteBuffer.allocate(digest.smallByteSize());
digest.asSmallBytes(buf);
buf.flip();
```
Файл: `TDigestAggSerde.java`, serializer.

### 2. OtlpSpanParser: фильтровать kind=2 AND kind=5
Изначально фильтровался только `kind=2` (SERVER). Kafka consumer spans (`kind=5`) содержат основную латентность (500ms в payment-service). Нужны оба типа.
Файл: `OtlpSpanParser.java`, константа `SPAN_KIND_CONSUMER = 5`.

### 3. AlertmanagerClient: /api/v1/alerts удалён в 0.28.0
Alertmanager 0.27.0 ещё принимал v1, но отвечал `410 Gone`. Правильный endpoint: `/api/v2/alerts`.
Файл: `AlertmanagerClient.java`.

### 4. OTel Collector healthcheck: нет shell в scratch-образе
`test: ["CMD-SHELL", "wget ..."]` падает — в образе нет shell. Правильно:
```yaml
test: ["CMD", "/otelcol-contrib", "--version"]
```
Файл: `docker-compose.yml`.

### 5. kafka_consumer_group_lag vs kafka_consumergroup_lag
Метрика kafka-exporter называется `kafka_consumergroup_lag` (без подчёркивания между consumer и group). Запросы `kafka_consumer_group_lag` возвращают пустой результат.
Файлы: `prometheus/alert-rules.yml`, `grafana/provisioning/dashboards/eda-overview.json`.

---

## 14. Как расширить систему

### Добавить новый тип алерта (например, ABSENCE — heartbeat)

1. Создать класс, реализующий `RuleHandler`:
   ```java
   @Component
   public class HeartbeatMonitor implements RuleHandler {
       @Override public String getRuleType() { return "ABSENCE"; }

       @Scheduled(fixedDelay = 60_000)
       public void checkHeartbeat() {
           // Проверить, что сервис посылал спаны в последние N минут
           // Если нет — создать AlertEvent и вызвать alertmanager.fire()
       }
   }
   ```
2. Параметры вынести в `MonitoringProperties` и `application.yml`.
3. Никакого изменения в LatencyTopology или AlertSink не нужно.

### Добавить новую панель в Grafana

Отредактировать `grafana/provisioning/dashboards/eda-overview.json`. После `make up` или перезапуска Grafana панель появится автоматически.

### Включить Slack-уведомления

1. Задать переменную окружения `SLACK_WEBHOOK_URL=https://hooks.slack.com/...`
2. Раскомментировать `slack_api_url` и `slack_configs` в `alertmanager/config.yml`.
3. Перезапустить alertmanager: `docker restart alertmanager`.

---

## 15. Алгоритмы — полное описание

Система использует пять алгоритмов. Каждый описан с принципом работы, параметрами и конкретными числами из конфигурации.

---

### Алгоритм 1: Hopping Window (скользящее окно Kafka Streams)

**Где:** `LatencyTopology.java`, метод `latencyStream()`
**Параметры:** `windowSize=60s`, `advanceBy=10s`, `grace=10s`

#### Принцип

Hopping window — разновидность скользящего окна с фиксированным шагом. В отличие от tumbling window (непересекающиеся окна) и sliding window (окно двигается на каждое событие), hopping window создаёт **перекрывающиеся окна фиксированного размера**, сдвигаемые на константный шаг.

#### Визуализация

```
Временная ось →
t=  0  10  20  30  40  50  60  70  80  90 100

Окно A: [0───────────────────────60)         (открылось в t=0)
Окно B:      [10──────────────────────70)    (открылось в t=10)
Окно C:           [20───────────────────80)  (открылось в t=20)
Окно D:                [30──────────────────90)
Окно E:                     [40─────────────────100)
Окно F:                          [50──────────────────110)

Спан пришёл в t=55 → он попадает в окна C, D, E, F (4 из 6 возможных)
Спан пришёл в t=5  → он попадает только в окно A (единственное открытое)
```

**Количество активных окон одновременно** = `windowSize / advanceBy` = `60 / 10` = **6 окон**.

#### Как Kafka Streams реализует это

Kafka Streams хранит агрегат (T-Digest) для каждого окна в **RocksDB state store** (`latency-tdigest`). Ключ state store: `(serviceName, windowStart, windowEnd)`.

При поступлении нового SpanRecord с временной меткой `t`:
1. Находятся все активные окна, охватывающие `t`: от `[t - windowSize + 1 ... t]` с шагом `advanceBy`.
2. Для каждого из этих окон вызывается aggregator: `agg.add(durationMs, traceId)`.
3. После каждого добавления результат (обновлённый T-Digest) сохраняется в state store.

Kafka Streams **не ждёт закрытия окна** — он испускает результаты непрерывно по мере поступления данных (eager evaluation). Это значит: при каждом новом спане `.toStream()` испускает **обновлённое** состояние каждого затронутого окна.

#### Параметр grace

`grace=10s` задаёт время ожидания опоздавших событий. Если спан с `t=50` пришёл в реальном времени `t=65` (опоздал на 15s) — он будет проигнорирован для уже закрытых окон. Kafka использует **event time** (из `startTimeUnixNano` спана), а не processing time.

#### Почему может быть несколько одинаковых алертов за короткое время

При `p99 > 200ms` и активных 6 окнах — `filter` пропускает сразу 6 обновлений (по одному на каждое окно). Каждое порождает вызов `alertmanager.fire()`. Alertmanager дедуплицирует их, т.к. `labels` у всех одинаковые.

```java
// Код в LatencyTopology.java
TimeWindows.ofSizeAndGrace(
    Duration.ofSeconds(60),   // windowSize
    Duration.ofSeconds(10)    // grace
).advanceBy(Duration.ofSeconds(10))  // hopping step
```

---

### Алгоритм 2: T-Digest (потоковое вычисление квантилей)

**Где:** `TDigestAgg.java`, `TDigestAggSerde.java`
**Библиотека:** `com.tdunning:t-digest:3.3`, класс `MergingDigest`
**Параметр:** `compression=100`

#### Зачем T-Digest, а не просто накопление точек

Для вычисления точного p99 нужно сортировать все N точек — это O(N log N) памяти и времени. При потоке в тысячи спанов в секунду это неприемлемо. T-Digest решает задачу в O(compression) памяти с гарантированной точностью.

#### Принцип алгоритма

T-Digest хранит данные в виде **центроидов** — кластеров из нескольких близких значений. Каждый центроид: `(mean, weight)`, где `mean` — среднее значение в кластере, `weight` — количество точек.

Центроиды расположены в отсортированном порядке по `mean`. Количество точек в центроиде ограничено **функцией размера** (size function), которая зависит от позиции центроида в распределении:

```
max_weight(q) = 4 * n * q * (1 - q) / compression
```

где `q` — квантиль центроида (от 0 до 1), `n` — общее число точек, `compression=100`.

**Ключевое свойство:** при `q` близком к 0 или 1 (хвосты) `max_weight` → 0, поэтому хвостовые центроиды маленькие и точные. При `q=0.5` (медиана) центроиды крупные и менее точные. Для p99 это идеально.

#### Алгоритм добавления точки

```
add(x):
  1. Найти ближайший центроид c_min с минимальным |c_min.mean - x|
  2. Вычислить q = (накопленный вес до c_min) / total_weight
  3. Если c_min.weight < max_weight(q): добавить x к c_min (обновить mean и weight)
  4. Иначе: создать новый центроид (mean=x, weight=1)
  5. Периодически: слить близкие маленькие центроиды (merge pass)
```

#### Алгоритм вычисления квантиля p99

```
quantile(0.99):
  1. target_weight = 0.99 * total_weight       // нас интересует 99-й перцентиль
  2. Итерировать центроиды в порядке возрастания mean
  3. Накапливать weight: cumulative += c.weight
  4. Когда cumulative >= target_weight:
     5. Интерполировать между предыдущим и текущим центроидом
     6. Вернуть интерполированное значение
```

#### Параметр compression=100

Управляет числом центроидов: примерно `compression * π / 2 ≈ 157` центроидов максимум. Точность p99 при `compression=100` и N≥100 точках: ±0.5–1%.

#### Сериализация для Kafka state store

Kafka Streams персистирует TDigestAgg в RocksDB и replication changelog-топик. `TDigestAggSerde` использует "small format" T-Digest:

```java
// Сериализация (smallByteSize/asSmallBytes — compact binary format)
ByteBuffer tBuf = ByteBuffer.allocate(digest.smallByteSize());
digest.asSmallBytes(tBuf);
tBuf.flip();

// Десериализация
MergingDigest.fromBytes(ByteBuffer.wrap(tBytes));
```

---

### Алгоритм 3: Tail-Based Sampling (OTel Collector)

**Где:** `otel/collector-config.yaml`, секция `tail_sampling`
**Параметры:** `decision_wait=10s`, `num_traces=10000`, `policies=[always_sample]`

#### Зачем tail sampling

Альтернатива — **head sampling**: решение принимается при первом спане трейса (в order-service). Проблема: первый спан (HTTP, ~10ms) нормальный, но мы не знаем, что дальше payment-service потратит 500ms. Медленные трейсы могут быть отброшены.

**Tail sampling** откладывает решение до получения всего трейса. Коллектор видит полную картину и может применять политики на основе результата (`has_error`, `latency > X ms`, `probabilistic`).

#### Алгоритм

```
При получении спана:
  1. Определить traceId спана
  2. Найти или создать запись в буфере: buffer[traceId]
  3. Добавить спан в buffer[traceId].spans
  4. Если buffer[traceId] ещё не получил решение: взвести таймер decision_wait

По истечении decision_wait для traceId:
  5. Применить policies по порядку:
     - always_sample → SAMPLE (100%)
     - (для prod: status_code=ERROR → SAMPLE; probabilistic 10% → SAMPLE/DROP)
  6. Если решение SAMPLE:
     7. Экспортировать все буферизованные спаны этого трейса → Jaeger + Kafka
  8. Если решение DROP:
     9. Удалить буфер этого трейса
  10. Освободить запись из buffer[traceId]

При переполнении буфера (num_traces=10000):
  11. Вытеснить самый старый трейс (LRU) с принудительным решением
```

#### Важность порядка процессоров

```yaml
processors: [memory_limiter, resource, tail_sampling, batch]
#                                            ↑
#                           ДОЛЖЕН быть перед batch
```

Если `batch` стоит перед `tail_sampling` — он отправит спаны downstream до того, как `tail_sampling` соберёт полный трейс. В `tail_sampling` попадут неполные данные → решение будет неправильным.

---

### Алгоритм 4: histogram_quantile (Prometheus p99)

**Где:** Grafana dashboard (`eda-overview.json`), Prometheus alert rules (`alert-rules.yml`)
**Метрики:** `otel_http_server_request_duration_seconds_bucket{le="..."}`

#### Как OTel-агент создаёт гистограмму

OTel Java-агент экспортирует HTTP-латентность как **гистограмму с предопределёнными buckets**. Каждый bucket `{le="X"}` содержит счётчик запросов, выполненных за ≤X секунд:

```
le="0.005"  → кол-во запросов ≤5ms
le="0.01"
le="0.025"
le="0.05"
le="0.075"
le="0.1"
le="0.25"
le="0.5"    → кол-во запросов ≤500ms
le="0.75"
le="1.0"
le="+Inf"   → все запросы (total)
```

#### Алгоритм histogram_quantile(0.99, ...)

Prometheus вычисляет квантиль по накопленным счётчикам bucket-ов методом **линейной интерполяции внутри bucket**:

```
Входные данные (после rate()):
  le=0.25:  rate=0.8/s  (80% запросов быстрее 250ms)
  le=0.5:   rate=0.95/s (95% запросов быстрее 500ms)
  le=0.75:  rate=0.99/s (99% запросов быстрее 750ms)
  le=1.0:   rate=1.0/s  (100%)

Цель: найти X, при котором 99% запросов < X

1. target = 0.99 * rate(le="+Inf") = 0.99 * 1.0 = 0.99
2. Найти два соседних bucket: le=0.5 (rate=0.95) и le=0.75 (rate=0.99)
   target=0.99 попадает в [0.95, 0.99]
3. Линейная интерполяция:
   fraction = (0.99 - 0.95) / (0.99 - 0.95) = 1.0
   p99 = 0.5 + fraction * (0.75 - 0.5) = 0.5 + 0.25 = 0.75 секунды = 750ms
```

#### Запрос в Grafana dashboard

```promql
histogram_quantile(0.99,
  sum(rate(otel_http_server_request_duration_seconds_bucket{
    http_route!~"/actuator.*"
  }[2m]))
  by (le, service_name)
)
```

`rate([2m])` — скорость изменения каждого bucket за последние 2 минуты (нормализует к секунде).
`sum(...) by (le, service_name)` — суммирует по всем инстансам, сохраняя `le` и `service_name`.

#### Ограничение: точность зависит от bucket boundaries

Если p99 находится в bucket `(0.5, 1.0]`, и реальное значение 502ms — Prometheus может вернуть любое значение между 500ms и 1000ms (интерполяция линейная, не точная). Именно поэтому Grafana dashboard и Prometheus rules используют `histogram_quantile` для индикации, а точное значение (504ms) даёт T-Digest в monitoring-service.

---

### Алгоритм 5: rate() и irate() (Prometheus)

**Где:** `prometheus/alert-rules.yml`, `eda-overview.json`

#### rate(metric[window])

Вычисляет среднюю скорость изменения counter-метрики за указанный период.

```
rate(kafka_consumergroup_lag[5m]):
  1. Взять все точки метрики за последние 5 минут
  2. Линейная регрессия (least squares) по этим точкам
  3. Вернуть наклон прямой (delta / seconds)
```

`rate()` устойчив к единичным выбросам и сбросам счётчика (counter reset detection).

#### Пример из DLQ alert rule

```promql
sum(rate(kafka_topic_partition_current_offset{topic=~".*dlq.*"}[2m])) by (topic) > 0.01
```

Если в DLQ-топик поступает хотя бы 1 сообщение за 100 секунд — `rate > 0.01` → алерт.

#### Как работает consumer lag в Prometheus

Kafka Exporter экспортирует `kafka_consumergroup_lag` как gauge (не counter — значение само по себе является текущим лагом). Поэтому для него используется прямое сравнение, а не rate:

```promql
kafka_consumergroup_lag > 1000   # прямое сравнение gauge
```

---

### Взаимодействие алгоритмов: полный путь вычисления p99

```
Спан (durationMs=502ms) поступает из Kafka

    ┌─ Hopping Window определяет: этот спан принадлежит окнам
    │   W1=[t-60, t), W2=[t-50, t+10), ..., W6=[t-10, t+50)
    │
    ├─ Для каждого окна Wi:
    │   └─ T-Digest.add(502ms)
    │        ├─ Найти ближайший центроид
    │        ├─ Если не переполнен: объединить точку с центроидом
    │        └─ Иначе: создать новый центроид (502ms, weight=1)
    │
    ├─ filter: T-Digest.quantile(0.99) > 200ms?
    │   └─ Итерация центроидов → линейная интерполяция → 504ms > 200ms ✓
    │
    └─ AlertEvent: service=payment-service, p99=504ms, traceId=abc123

Параллельно в Prometheus:
    ├─ histogram_quantile(0.99, rate(otel_http_server_..._bucket[2m]))
    │   └─ Bucket interpolation → ~500-750ms (менее точно, зависит от bucket bounds)
    │
    └─ ConsumerLagMonitor polling: kafka_consumergroup_lag
        └─ direct gauge comparison → lag > 1000? (независимая проверка)
```

---

### Сводная таблица алгоритмов

| Алгоритм | Где | Сложность | Точность | Назначение |
|----------|-----|-----------|----------|-----------|
| Hopping Window | LatencyTopology.java | O(windowSize/step) окон | Точно | Группировка спанов по времени |
| T-Digest | TDigestAgg.java | O(compression) памяти | ±0.5–1% для p99 | Потоковый p99 без хранения всех точек |
| Tail Sampling | collector-config.yaml | O(num_traces) буфер | 100% (always) | Репрезентативная выборка трейсов |
| histogram_quantile | Prometheus / Grafana | O(buckets) | Зависит от bucket bounds | p99 из метрик OTel-агента |
| rate() | Prometheus alert rules | O(scrape points) | Среднее за окно | Скорость изменения DLQ-offset |

```bash
# Активные алерты
curl -s http://localhost:9093/api/v2/alerts | python3 -m json.tool

# Prometheus query
curl -s "http://localhost:9090/api/v1/query?query=up" | python3 -m json.tool

# Consumer lag
curl -s "http://localhost:9090/api/v1/query?query=kafka_consumergroup_lag" | python3 -m json.tool

# Логи из Loki (последние 10 минут)
curl -s "http://localhost:3100/loki/api/v1/query_range" \
  --data-urlencode 'query={service_name="payment-service"}' \
  --data-urlencode 'start=-10m' | python3 -m json.tool

# Топики в Kafka
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --list

# Consumer groups
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --list

# Lag конкретной группы
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --group monitoring-service --describe
```
