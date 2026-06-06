# Monitor-System — Руководство по запуску и наблюдению

## Часть 1. Запуск

### Предварительные условия

- Docker и Docker Compose v2
- `make`
- Клонированный репозиторий `order-system` в `/home/alex/projects/order-system`

---

### Первый запуск (один раз)

**Шаг 1.** Скачать OTel Java-агент:

```bash
cd /home/alex/projects/monitor-system
make agent
```

Загружает `otel/opentelemetry-javaagent.jar` (~21 МБ). В git не коммитится, нужен только локально.

**Шаг 2.** Поднять весь стек:

```bash
make up
```

Запускает 15 контейнеров: order-system (4 Java-сервиса + Kafka + Postgres) и стек наблюдаемости (OTel Collector, Jaeger, Prometheus, Loki, Grafana, Alertmanager, Kafka Exporter, monitoring-service).

**Шаг 3.** Дождаться готовности (~60 секунд):

```bash
make ps
```

Все контейнеры должны показывать статус `(healthy)`. Java-сервисы (`order-service`, `payment-service` и др.) показывают просто `Up` — это нормально, у них нет healthcheck.

---

### Повторный запуск

```bash
make up      # идемпотентно — запустит остановленные контейнеры без пересборки
```

### Остановка (с удалением данных)

```bash
make down    # останавливает всё и удаляет тома Prometheus, Grafana, Loki
```

---

### Адреса интерфейсов

| URL | Описание |
|-----|----------|
| `http://localhost:3000` | Grafana (логин: admin / пароль: admin) |
| `http://localhost:16686` | Jaeger — просмотр трейсов |
| `http://localhost:9090` | Prometheus — метрики и графики |
| `http://localhost:9093` | Alertmanager — активные алерты |
| `http://localhost:3100` | Loki — хранилище журналов |
| `http://localhost:8090` | Kafka UI — топики и сообщения |
| `http://localhost:8085` | monitoring-service — actuator/health |

---

## Часть 2. Проверка работоспособности после запуска

После запуска `make anomaly-latency` выполни шаги ниже по порядку.

```bash
make anomaly-latency
# Отправляет 100 параллельных POST /orders — имитирует всплеск нагрузки.
# Засеки время: алерты появятся через ~30–60 секунд.
```

---

### Шаг 1 — Kafka UI: спаны попали в топик

**Открыть:** `http://localhost:8090` → Topics → `otel.spans`

**Что проверить:**
- Поле **Messages** растёт (должно быть > 0 через 15 секунд после burst).
- **Last message timestamp** — свежий.

Это подтверждает, что OTel Collector получил трейсы от Java-агента и переправил их в Kafka для обработки в monitoring-service.

**Если Messages = 0 через 15 секунд:**
```bash
docker logs otel-collector --tail 20
docker logs order-service --tail 20 | grep -i "otel\|agent"
```

---

### Шаг 2 — Jaeger: полный трейс EDA-цепочки

**Открыть:** `http://localhost:16686`

**Действия:**
1. В поле **Service** выбрать `order-service`.
2. Нажать **Find Traces**.
3. Найти трейс с **≥ 7 спанов** — это полная цепочка через все сервисы.
4. Раскрыть трейс — ожидаемая иерархия:

```
order-service      POST /orders               ~10ms
└─ order-service   orders publish             ~15ms
   └─ payment-service   orders process        ~500ms  ← медленно
      └─ payment-service   payments publish   ~2ms
         └─ shipping-service  payments process  ~200ms
            └─ shipping-service  shipments publish  ~2ms
               └─ analytics-service  (несколько спанов)
```

5. `payment-service: orders process` должен иметь красную полосу длиной > 200ms.

**На что обратить внимание:** все спаны одного трейса связаны через W3C `traceparent` в заголовках Kafka-сообщений — это и есть сквозная трассировка без изменения кода.

---

### Шаг 3 — Grafana: дашборд EDA Overview

**Открыть:** `http://localhost:3000` → Dashboards → **EDA Overview**

Установить диапазон времени: **Last 5 minutes** (кнопка в правом верхнем углу).

**Что смотреть на каждой панели:**

| Панель | Ожидаемое значение |
|--------|-------------------|
| **p99 latency** | Всплеск до ~500ms для `payment-service`, ~200ms для `shipping-service` |
| **Consumer lag** | Временный рост на топике `orders`, затем возврат к 0 |
| **DLQ rate** | 0 — нет ошибок при нормальном burst |
| **Service status** | Все сервисы = 1 (зелёный) |
| **Error rate** | 0 или минимальный |

**Exemplars (точки-ромбики на графике p99):**
Кликнуть на ромбик → откроется Jaeger с конкретным трейсом, соответствующим этой точке метрики. Это и есть связь метрика → трейс.

---

### Шаг 4 — Alertmanager: алерты сработали

**Открыть:** `http://localhost:9093`

Через 30–60 секунд после burst должны появиться два алерта:

```
[warning] THRESHOLD_payment_service    p99=504ms > threshold=200ms (n=21)
[warning] THRESHOLD_shipping_service   p99=203ms > threshold=200ms (n=21)
```

Алерты имеют TTL 5 минут — через 5 минут после последнего firing они исчезнут.

**Если алертов нет, проверить логи:**
```bash
docker logs monitoring-service --tail 30 | grep -E "ALERT|fired|Failed"
```

Ожидаемые строки в логах:
```
[ALERT] THRESHOLD service=payment-service p99=504ms
Alert fired: service=payment-service rule=THRESHOLD status=200 OK
```

---

### Шаг 5 — Loki: журналы с привязкой к трейсу

**Открыть:** Grafana → **Explore** → источник данных **Loki**

**Запрос:**
```logql
{service_name="payment-service"} | json | line_format "{{.body}}"
```

В строках журнала будет поле `trace_id`. Кликнуть на него → ссылка **Derived Field** перебросит в Jaeger на соответствующий трейс. Это связь журнал → трейс.

**Запрос для просмотра всех сервисов сразу:**
```logql
{service_name=~"order-service|payment-service|shipping-service"}
```

---

### Шаг 6 — Prometheus: сырые метрики и exemplars

**Открыть:** `http://localhost:9090` → вкладка **Graph**

**Запрос p99 по сервисам:**
```promql
histogram_quantile(0.99,
  sum(rate(otel_http_server_request_duration_seconds_bucket[2m]))
  by (le, service_name)
)
```

**Запрос consumer lag:**
```promql
sum(kafka_consumergroup_lag) by (topic, consumergroup)
```

На графике p99 включить отображение **Exemplars** (переключатель под полем запроса) — появятся ромбики, клик открывает Jaeger.

---

### Сводная временная шкала

```
t=0s    make anomaly-latency        — 100 параллельных запросов отправлены
t=5s    Java-агент отправил спаны в OTel Collector (OTLP gRPC :4317)
t=15s   Collector принял решение по tail_sampling, экспортировал в Kafka
t=15s   otel.spans содержит новые сообщения (виден в Kafka UI)
t=20s   Kafka Streams обработал спаны, рассчитал T-Digest p99
t=30s   Первое скользящее окно (60s/10s) завершено → алерты в Alertmanager
t=60s   Grafana показывает всплеск на панели p99 latency
t=300s  Алерты истекают (TTL 5 минут), исчезают из Alertmanager
```

---

### Полезные команды для диагностики

```bash
# Статус всех контейнеров
make ps

# Логи monitoring-service (Kafka Streams + алерты)
docker logs monitoring-service -f

# Логи OTel Collector (приём и экспорт спанов)
docker logs otel-collector -f

# Проверить, что топики созданы
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --list

# Consumer lag monitoring-service по топику otel.spans
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --group monitoring-service --describe

# Активные алерты через API
curl -s http://localhost:9093/api/v2/alerts | python3 -m json.tool
```
