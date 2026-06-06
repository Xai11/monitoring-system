# Итоговая таблица компонентов и заключение раздела 5

## Таблица компонентов monitoring-service

| Класс | Алгоритм / паттерн | Сложность | Назначение |
|---|---|---|---|
| `OtlpSpanParser` | Линейный разбор JSON-дерева; фильтрация по SpanKind и имени эндпоинта | O(S) по числу спанов S в пакете | Десериализация OTLP JSON-пакетов из топика `otel.spans` в плоский список `SpanRecord`; отбрасывание нерелевантных спанов (PRODUCER, Actuator, health) до попадания в стрим |
| `TDigestAgg` | T-Digest (MergingDigest, compression=100); скользящее буферизованное слияние центроидов | Добавление: O(log C) амортизированно; квантиль: O(C); память: O(C), C ≤ 157 | Потоковое вычисление p99 задержки на скользящем окне 60 с / шаг 10 с без хранения исходных наблюдений; хранение exemplar `lastTraceId` |
| `TDigestAggSerde` | Бинарная сериализация «small format» + length-prefixed UTF-8; паттерн Adapter | O(C) по числу центроидов C; типичный размер ≈ 1,3 КБ | Сериализация/десериализация `TDigestAgg` для RocksDB state store и changelog-топика Kafka Streams; обеспечивает отказоустойчивость агрегата между перезапусками |
| `LatencyTopology` | DAG Kafka Streams: flatMapValues → filter → selectKey (repartition) → groupByKey → hopping window → aggregate → toStream (eager) → filter → map → to; паттерн Template Method через `RuleHandler` | O(log C) на каждый входящий спан; repartition — O(1) по сети | Главный конвейер правила THRESHOLD: от raw-спана до `AlertEvent` в топике `otel.alerts.pending`; вычисление p99 и проверка двойного условия size ≥ 10 && p99 > 200 мс |
| `ConsumerLagMonitor` | Polling Prometheus HTTP API; PromQL Instant Query; паттерн Strategy через `MetricCallback`; `@Scheduled` fixed delay | O(G) по числу групп-потребителей G; O(T) по числу DLQ-топиков T | Два независимых правила: THRESHOLD (consumer lag, опрос каждые 10 с) и RATE (DLQ-частота через `rate()` offset-метрики, опрос каждые ~8 мин); не требует Kafka Streams |
| `AlertmanagerClient` | HTTP POST `/api/v2/alerts`; дедупликация на стороне Alertmanager по fingerprint (SHA-256 от labels); TTL через `endsAt = firedAt + 300 с` | O(1) — один HTTP-запрос на алерт | Диспетчеризация `AlertEvent` в Alertmanager; формирование payload с labels (ключ дедупликации), annotations (summary, trace_id), управление жизненным циклом алерта |
| `AlertSink` | Kafka Consumer (`@KafkaListener`); паттерн Idempotent Receiver | O(1) на сообщение | Централизованный потребитель топика `otel.alerts.pending`; точка доставки для правил RATE/ABSENCE, публикующих алерты в топик в обход прямого вызова `alertmanager.fire()` |
| `RuleHandler` | Паттерн Plugin Interface (Open/Closed Principle) | — | Контракт расширения: новый тип правила добавляется реализацией интерфейса без изменения кода детектора; метод `getRuleType()` связывает реализацию с полем `AlertEvent.ruleType` и меткой Alertmanager |

---

## Заключение раздела 5

Совокупность описанных компонентов образует единый детерминированный конвейер от
сырого телеметрического события до уведомления дежурного. Конвейер берёт начало в
момент, когда OTel Java-агент инжектирует трассировочный контекст W3C TraceContext
в байткод Kafka-клиента целевого сервиса и публикует OTLP JSON-пакет в топик
`otel.spans`. Класс `OtlpSpanParser` десериализует пакет и отбирает релевантные
спаны, `LatencyTopology` накапливает их в T-Digest агрегате `TDigestAgg` внутри
скользящего окна, сохраняемого в RocksDB через `TDigestAggSerde`. При каждом шаге
окна (каждые 10 с) оператор `toStream()` в режиме eager evaluation материализует
текущее состояние агрегата: если выполнено двойное условие статистической
надёжности и превышения порога, `AlertmanagerClient` формирует payload с labels
для дедупликации и полем `trace_id` для навигации к конкретному трейсу, после чего
выполняет HTTP POST к `/api/v2/alerts`. Параллельно `ConsumerLagMonitor` по
расписанию опрашивает Prometheus API и применяет правила RATE и THRESHOLD к
метрикам инфраструктурного уровня, публикуя результаты через `AlertSink` в тот же
топик `otel.alerts.pending`. Интерфейс `RuleHandler` удерживает всю эту
архитектуру открытой для расширения: каждый из трёх реализованных типов правил —
THRESHOLD, RATE, ABSENCE — подключается без единого изменения в существующем коде
детектора. Сквозное время обнаружения аномалии — от момента ухудшения характеристик
сервиса до получения Slack-уведомления — составляет менее 10 секунд: оно
определяется шагом скользящего окна (10 с), а не длительностью полного окна (60 с),
благодаря семантике eager evaluation оператора `toStream()` в топологии Kafka Streams.
