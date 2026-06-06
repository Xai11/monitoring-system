# CLAUDE.md

Контекст и правила работы для Claude Code в этом репозитории. Прочитай файл целиком перед любым действием.

## 1. Что это за проект

Система наблюдаемости (monitor-system) для событийно-ориентированной среды (EDA) на базе Apache Kafka. Цель — обеспечить сквозную трассировку событий, сбор метрик, журналов и обнаружение аномалий в реальном времени для уже существующего приложения, **не изменяя его код**.

Решение монолитное по способу поставки (один `docker compose up` поднимает весь стек), но модульное по структуре: каждый слой обработки телеметрии изолирован в собственном каталоге с декларативной конфигурацией. Слои абстрагированы друг от друга и общаются только через объявленные интерфейсы (OTLP, Prometheus scrape, LogQL/TraceQL). Замена одного компонента (например, Jaeger → Tempo) не должна затрагивать остальные.

## 2. ГЛАВНОЕ ОГРАНИЧЕНИЕ — не трогать целевое приложение

Целевое приложение находится в `/project/order-system`. Это событийно-ориентированный сервис заказов.

**Запрещено:** создавать, изменять или удалять любые файлы внутри `/project/order-system`. Не редактировать его исходники, `pom.xml`/`build.gradle`, его `Dockerfile`, его `docker-compose.yml`, его `application.yml`. Не добавлять туда зависимости и аннотации.

Наблюдаемость подключается **только снаружи**, неинвазивно:
- авто-инструментация через OpenTelemetry Java-агент (`-javaagent`), инжектируемый в JVM переменными окружения при запуске контейнера;
- внешний сбор метрик брокера (Kafka Exporter), работающий по протоколу Kafka/JMX и не требующий доступа к коду;
- Docker Compose override-файл, который добавляет переменные окружения и volume-монтирование к сервисам `order-system`, **при этом сам override-файл лежит в нашем каталоге `monitor-system/`, а не в каталоге приложения**.

Если какая-то задача кажется невыполнимой без правки `order-system` — остановись и сообщи об этом, предложив неинвазивную альтернативу. Не редактируй их файлы «временно».

## 3. Где мы работаем

Весь наш код и конфигурации создаём ТОЛЬКО в каталоге `/project/monitor-system/`. Целевая структура:

```
/project/monitor-system
├── CLAUDE.md                         ← этот файл
├── docker-compose.yml                ← стек наблюдаемости
├── docker-compose.order-system.yml   ← override: инжектирует OTel-агент в сервисы order-system
├── Makefile                          ← команды запуска и демо-сценариев
├── otel/
│   ├── opentelemetry-javaagent.jar   ← скачивается, в git не коммитим (см. Makefile target `agent`)
│   └── collector-config.yaml         ← receivers/processors/exporters, tail_sampling
├── prometheus/
│   ├── prometheus.yml                ← scrape_configs (collector :8889, kafka-exporter :9308)
│   └── alert-rules.yml               ← ConsumerLagHigh, P99LatencyBreach, DlqRateHigh, ServiceDown
├── alertmanager/
│   └── config.yml                    ← маршрутизация по severity, Slack receiver (опц.)
├── loki/
│   └── config.yaml                   ← локальное хранилище, retention 7 дней
└── grafana/
    └── provisioning/
        ├── datasources/all.yml       ← Prometheus + Jaeger + Loki, Exemplars + Derived Fields
        └── dashboards/
            ├── dashboards.yml        ← автозагрузка дашбордов
            └── eda-overview.json     ← главный дашборд (минимальный результат)
```

## 4. Технологический стек (фиксированный)

| Слой | Технология | Назначение |
| --- | --- | --- |
| Сбор телеметрии | OpenTelemetry Java agent + OTel Collector (contrib) | Zero-code инструментация, OTLP gRPC :4317, tail-based sampling |
| Трассировки | Jaeger (all-in-one) :16686 | Хранение и просмотр трейсов |
| Метрики | Prometheus :9090 | TSDB, scrape интервал 15 с |
| Consumer lag | Kafka Exporter :9308 | `kafka_consumer_group_lag` из брокера order-system |
| Журналы | Grafana Loki :3100 | Индекс по лейблам, полнотекст через LogQL |
| Визуализация | Grafana :3000 | Дашборды, переходы метрика → трейс → журнал |
| Алертинг | Alertmanager :9093 (+ Slack webhook, опц.) | Маршрутизация, дедупликация |
| Обнаружение аномалий | monitoring-service (Kafka Streams) :8084 | Скользящее окно p99, DLQ rate — слой 2, см. §7 |
| Оркестрация | Docker Compose | Запуск всего стека одной командой |

Все компоненты выполняются в одной Docker-сети. Persist-данные — в именованных томах (`prometheus_data`, `grafana_data`, `loki_data`).

## 5. Как агент подключается без правки кода

Override-файл `docker-compose.order-system.yml` для каждого сервиса `order-system` добавляет (НЕ переопределяя их образы и команды):

```yaml
services:
  order-service:        # имена должны совпадать с сервисами из order-system/docker-compose.yml
    volumes:
      - ./otel/opentelemetry-javaagent.jar:/otel/opentelemetry-javaagent.jar:ro
    environment:
      JAVA_TOOL_OPTIONS: "-javaagent:/otel/opentelemetry-javaagent.jar"
      OTEL_SERVICE_NAME: "order-service"
      OTEL_EXPORTER_OTLP_ENDPOINT: "http://otel-collector:4317"
      OTEL_EXPORTER_OTLP_PROTOCOL: "grpc"
      OTEL_TRACES_SAMPLER: "parentbased_always_on"
      OTEL_INSTRUMENTATION_KAFKA_EXPERIMENTAL_SPAN_ATTRIBUTES: "true"
      OTEL_METRICS_EXPORTER: "otlp"
      OTEL_LOGS_EXPORTER: "otlp"
```

Запуск всей системы выполняется объединением обоих compose-файлов:
```
docker compose \
  -f /project/order-system/docker-compose.yml \
  -f /project/monitor-system/docker-compose.order-system.yml \
  -f /project/monitor-system/docker-compose.yml \
  up -d
```
Файл `order-system/docker-compose.yml` передаётся флагом `-f` только на чтение — Compose сливает конфигурации в памяти и НЕ изменяет файл на диске. Это и есть неинвазивное подключение: агент инжектирует W3C `traceparent`/`tracestate` в заголовки Kafka-сообщений на уровне байткода клиента Kafka, без единой строки в коде приложения.

Перед первым запуском нужно скачать агент (см. `make agent`): официальный `opentelemetry-javaagent.jar` с GitHub-релизов OpenTelemetry.

## 6. Минимальный результат (Definition of Done для прототипа)

Прототип считается рабочим, когда выполняется ВСЁ перечисленное:

1. `make up` поднимает весь стек и сервисы order-system без ошибок; `docker compose ps` показывает все контейнеры `healthy`.
2. Grafana доступна на `http://localhost:3000` (admin/admin), и при первом входе **уже провизионирован** дашборд `EDA Overview` — без ручной настройки через UI.
3. Дашборд `EDA Overview` показывает живые данные при работающем order-system: panel «p99 latency» (с exemplars), «Consumer lag по топикам», «DLQ rate», «Статус сервисов (up/down)».
4. В Jaeger (`http://localhost:16686`) видны трейсы order-system, проходящие через Kafka, с ≥ 2 спанами (producer → consumer), связанными контекстом трассировки.
5. Источники данных Prometheus, Jaeger, Loki в Grafana зарегистрированы автоматически; настроены Exemplars (метрика → трейс) и Derived Fields в Loki (журнал → трейс по `trace_id`).

Пункт 2 и 3 (живой дашборд из коробки) — обязательный видимый результат. Реализуй сначала эту цепочку целиком, прежде чем браться за слой обнаружения аномалий.

## 7. Слой обнаружения аномалий (после того, как дашборд заработал)

`monitoring-service` на Kafka Streams читает поток метрик латентности и обнаруживает инциденты. Параметры выносятся в конфигурацию (без перекомпиляции):

```yaml
monitoring.latency.window-size-seconds: 60   # скользящее окно
monitoring.latency.slide-seconds: 10         # шаг пересчёта
monitoring.latency.p99-threshold-ms: 200     # порог
monitoring.latency.min-sample-count: 10      # отсекает ложные срабатывания на единичных выбросах
monitoring.dlq.window-size-minutes: 5
monitoring.dlq.rate-threshold: 0.01
monitoring.consumer-lag.threshold-messages: 1000
```

Алгоритм (THRESHOLD-правило): сгруппировать поток по `service_name` → на скользящем окне 60 с / шаг 10 с посчитать p99 через T-Digest → если `p99 > threshold` И `sample_count >= min_sample_count`, сформировать `AlertEvent` (service, p99, threshold, window, severity, exemplar_trace_id) → опубликовать в топик `otel.alerts.pending` → Alertmanager маршрутизирует по severity. Типы правил: `THRESHOLD`, `RATE` (частота, напр. DLQ), `ABSENCE` (heartbeat). Новый тип правила добавляется как новый обработчик `RuleHandler` без правки кода детектора.

Этот слой — расширение, а не часть минимального прототипа. Не блокируй им пункт 6.

## 8. Команды (Makefile)

Реализуй в `Makefile` как минимум:
- `make agent` — скачать `opentelemetry-javaagent.jar` в `otel/`.
- `make up` — собрать `-f`-цепочку из §5 и поднять всё (`up -d`).
- `make down` — `down -v` (с удалением томов).
- `make logs` — `logs -f` по нашему стеку.
- `make ps` — статус всех контейнеров.
- `make load` — простой генератор нагрузки/событий на order-system, чтобы дашборд ожил (через публичные эндпоинты/топики, без правки кода).
- `make anomaly-latency` — демо-сценарий: искусственная задержка, чтобы показать срабатывание алерта (нужен слой §7).

## 9. Конвенции и правила для Claude Code

- Декларативность прежде всего: вся конфигурация — версионируемые YAML/JSON-файлы, монтируемые в контейнеры через volume. Никаких ручных шагов настройки через UI, никаких императивных `docker exec`-команд в качестве части установки.
- Идемпотентность: повторный `make up` приводит систему к заданному состоянию, не плодя дубли.
- Имена сервисов в override (`docker-compose.order-system.yml`) обязаны совпадать с реальными именами сервисов в `order-system/docker-compose.yml`. Сначала прочитай их compose-файл (только чтение!), чтобы узнать имена сервисов, имя Kafka-брокера, топики и сеть. Если order-system использует собственную Docker-сеть — подключи наши контейнеры к ней через `networks: external: true`, не меняя их определение сети.
- Перед изменением любого файла убедись, что его путь начинается с `/project/monitor-system/`. Путь в `/project/order-system/` — стоп-сигнал.
- Не хардкодь секреты. Slack webhook и пароли — через переменные окружения / `.env` (в git не коммитим).
- Версии образов фиксируй явным тегом, не `latest`, для воспроизводимости.
- После значимого изменения проверяй результат: `make up`, затем `docker compose ps` и доступность Grafana, и сообщай фактический статус, а не предполагаемый.

## 10. Карта портов

| Порт | Сервис |
| --- | --- |
| 3000 | Grafana |
| 9090 | Prometheus |
| 16686 | Jaeger UI |
| 3100 | Loki |
| 9308 | Kafka Exporter |
| 9093 | Alertmanager |
| 4317 / 4318 | OTel Collector (OTLP gRPC / HTTP) |
| 8889 | OTel Collector (Prometheus exporter) |
| 8084 | monitoring-service (Kafka Streams, слой §7) |

## 11. Порядок выполнения первой задачи

1. Прочитай (только чтение) `/project/order-system/docker-compose.yml` и зафиксируй: имена сервисов, имя контейнера Kafka и его адрес, список топиков, имя Docker-сети, тип сборки (Java/Spring Boot предполагается).
2. Создай каталог `/project/monitor-system/` и в нём базовый стек §3 с минимальной конфигурацией.
3. Подними цепочку §6 и доведи до живого дашборда `EDA Overview`. Это и есть демонстрируемый результат.
4. Только после этого приступай к слою обнаружения аномалий §7.
