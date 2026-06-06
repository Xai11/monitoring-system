# Реализация топологии Kafka Streams для обнаружения аномалий латентности

## Листинг метода `latencyStream()`

Листинг 1 — `LatencyTopology.java`, метод `latencyStream()`

```java
@Bean
public KStream<String, String> latencyStream(StreamsBuilder builder) {
    var lat = props.getLatency();                              // (A) конфигурация из YAML

    // (B) источник: читаем все сообщения топика otel.spans
    KStream<String, String> raw = builder.stream(
        SPANS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

    raw
        // (C) один OTLP-пакет → 0..N записей SpanRecord
        .flatMapValues(value -> parser.parse(value))

        // (D) защитный барьер: убираем null и нулевые длительности
        .filter((k, span) -> span != null && span.durationMs() > 0)

        // (E) ключ перезначается на имя сервиса → триггер repartition
        .selectKey((k, span) -> span.serviceName())

        // (F) группировка по новому ключу (serviceName) с явными Serde
        .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(SpanRecord.class)))

        // (G) скачущее (hopping) окно: размер=60 с, шаг=10 с, grace=10 с
        .windowedBy(
            TimeWindows.ofSizeAndGrace(
                Duration.ofSeconds(lat.getWindowSizeSeconds()),
                Duration.ofSeconds(lat.getSlideSeconds())
            ).advanceBy(Duration.ofSeconds(lat.getSlideSeconds()))
        )

        // (H) агрегат T-Digest, персистируется в RocksDB + changelog
        .aggregate(
            TDigestAgg::new,
            (svc, span, agg) -> agg.add(span.durationMs(), span.traceId()),
            Materialized.<String, TDigestAgg, WindowStore<Bytes, byte[]>>
                as("latency-tdigest")
                .withKeySerde(Serdes.String())
                .withValueSerde(new TDigestAggSerde())
        )

        // (I) KTable → KStream: каждое обновление агрегата становится записью;
        //     ключ становится Windowed<String>, значение может быть null (tombstone)
        .toStream()

        // (J) двойное условие: статистическая надёжность + превышение порога
        .filter((wk, agg) ->
            agg != null
            && agg.size() >= lat.getMinSampleCount()
            && agg.p99() > lat.getP99ThresholdMs()
        )

        // (K) формирование AlertEvent + side effect: отправка в Alertmanager
        .map((wk, agg) -> {
            AlertEvent event = new AlertEvent(
                wk.key(),                               // service name
                getRuleType(),                          // "THRESHOLD"
                round2(agg.p99()),                      // фактический p99
                lat.getP99ThresholdMs(),                // порог из конфигурации
                "warning",
                String.format("p99=%.0fms > threshold=%.0fms (n=%d)",
                    agg.p99(), lat.getP99ThresholdMs(), agg.size()),
                agg.getLastTraceId(),                   // exemplar traceId
                Instant.now()
            );
            log.warn("[ALERT] {} service={} p99={}ms",
                getRuleType(), wk.key(), (int) agg.p99());
            alertmanager.fire(event);                   // HTTP POST /api/v2/alerts
            try {
                return KeyValue.pair(wk.key(), om.writeValueAsString(event));
            } catch (Exception e) {
                return KeyValue.pair(wk.key(), "{}");
            }
        })

        // (L) запись алертов в выходной топик
        .to(ALERTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

    return raw;  // (M) возвращаем исходный поток, не цепочку
}
```

---

## Возврат `raw` и природа построения топологии

Метод возвращает `raw` — исходный поток до какой-либо обработки, — а не конец
цепочки операторов. Это не ошибка, а следствие архитектуры Kafka Streams.

Вызовы операторов (`flatMapValues`, `filter`, `selectKey` и т. д.) **не выполняют
обработку данных** в момент вызова. Они являются декларативными описателями: каждый
вызов добавляет узел во внутренний граф (`ProcessorTopology`), хранящийся в объекте
`StreamsBuilder`. Фактическое выполнение начнётся позже — когда Spring Kafka
создаст экземпляр `KafkaStreams` и вызовет `KafkaStreams.start()`. Таким образом,
метод `latencyStream()` — это **фабрика топологии**, а не функция обработки.

Аннотация `@Bean` сигнализирует Spring: этот метод производит управляемый бин.
Spring вызовет его ровно один раз при старте контекста, тем самым «прошив» граф
операторов в `StreamsBuilder`. Возвращаемое значение `KStream<String, String> raw`
Spring регистрирует как бин, — это позволяет другим компонентам (если бы они были)
получить ссылку на исходный поток через `@Autowired`. Если бы метод возвращал
конец цепочки (результат `.to(...)`, который возвращает `void`), он не мог бы
быть `@Bean`-методом с осмысленным возвращаемым типом. Возврат `raw` — устойчивое
соглашение в Spring Kafka Streams: строй топологию как side effect, возвращай
«точку входа» потока.

---

## Пошаговый разбор операторов

### (C) `flatMapValues` — распаковка OTLP-пакета

```java
.flatMapValues(value -> parser.parse(value))
```

Каждое сообщение топика `otel.spans` содержит один OTLP JSON-пакет с потенциально
множеством спанов от нескольких сервисов. `flatMapValues` применяет функцию к
значению каждой записи и «разворачивает» возвращённый список: пакет с 12 спанами
порождает 12 отдельных записей в потоке. Ключ (`k`) при этом сохраняется
неизменным — раздел Kafka не меняется, дополнительный repartition не происходит.
Если `parser.parse()` вернул пустой список (все спаны отфильтрованы), запись
исчезает из потока без побочных эффектов.

### (D) `filter` — защитный барьер

```java
.filter((k, span) -> span != null && span.durationMs() > 0)
```

Первичная фильтрация по предметной логике выполнена внутри `OtlpSpanParser`.
Данный барьер является вторым рубежом защиты: он отсеивает гипотетические `null`-
значения, которые могут возникнуть при будущих расширениях `flatMapValues` (например,
если другой компонент подключит дополнительный маппер, не гарантирующий ненулевых
результатов), а также спаны с нулевой или отрицательной длительностью,
просочившиеся из нестандартных источников.

### (E) `selectKey` — смена ключа и repartition

```java
.selectKey((k, span) -> span.serviceName())
```

Исходный ключ записей в топике `otel.spans` — случайный строковый идентификатор,
устанавливаемый OTel Collector. Для корректной группировки по сервису ключ должен
быть равен `span.serviceName()`. Смена ключа через `selectKey` помечает поток как
«требующий repartition»: при следующем stateful-операторе (`groupByKey`) Kafka Streams
автоматически создаёт внутренний топик `monitoring-service-KSTREAM-KEY-SELECT-repartition`
и перераспределяет записи по его партициям согласно хэшу нового ключа. Это
гарантирует, что все спаны одного сервиса обрабатываются одним потоковым потоком
(stream task) и попадают в одно локальное хранилище T-Digest. Без этой гарантии
агрегат p99 был бы вычислен только по части наблюдений, что дало бы систематически
заниженные оценки перцентиля.

### (F) `groupByKey` — явные Serde

```java
.groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(SpanRecord.class)))
```

`Grouped.with(...)` задаёт Serde явно, обходя механизм автоопределения Kafka Streams.
Автоопределение потребовало бы настройки `default.value.serde` на уровне конфигурации
стрима для типа `SpanRecord`, что ограничило бы возможность одновременно использовать
другие типы значений в той же топологии. `JsonSerde<>(SpanRecord.class)` сериализует
`SpanRecord` как компактный JSON: `{"serviceName":"order-service","durationMs":213.0,"traceId":"4bf9..."}`.

### (G) `windowedBy` — скачущее (hopping) окно

```java
TimeWindows.ofSizeAndGrace(
    Duration.ofSeconds(lat.getWindowSizeSeconds()),   // 60 с — размер окна
    Duration.ofSeconds(lat.getSlideSeconds())          // 10 с — grace period
).advanceBy(Duration.ofSeconds(lat.getSlideSeconds())) // 10 с — шаг сдвига
```

Скачущее (hopping) окно отличается от скользящего (sliding) тем, что создаётся
фиксированным набором перекрывающихся окон с шагом `advanceBy`. При размере 60 с
и шаге 10 с каждое конкретное наблюдение входит ровно в ⌊60/10⌋ = 6 окон
одновременно. Это значит, что p99 пересчитывается каждые 10 с, используя данные
за последние 60 с.

**Grace period** (второй аргумент `ofSizeAndGrace`) — интервал, в течение которого
Kafka Streams принимает «опоздавшие» (late) записи после закрытия окна. Значение 10 с
соответствует допустимому отставанию часов между узлами кластера. Без grace period
опоздавшие записи отбрасывались бы молча, что приводило к систематическому
занижению выборки и ложным срабатываниям фильтра `minSampleCount`.

### (H) `aggregate` — накопление T-Digest в state store

```java
.aggregate(
    TDigestAgg::new,                                         // инициализатор
    (svc, span, agg) -> agg.add(span.durationMs(), span.traceId()),  // агрегатор
    Materialized.as("latency-tdigest")
        .withKeySerde(Serdes.String())
        .withValueSerde(new TDigestAggSerde())
)
```

Для каждой пары (serviceName, окно) Kafka Streams поддерживает отдельный экземпляр
`TDigestAgg` в state store `latency-tdigest`. При поступлении каждого спана
вызывается `agg.add(durationMs, traceId)`, который добавляет точку в `MergingDigest`
и обновляет `lastTraceId`. Персистентный `Materialized` state store на основе RocksDB
обеспечивает сохранность агрегата между перезапусками сервиса; одновременно каждое
изменение реплицируется в changelog-топик Kafka для восстановления после сбоя.

### (I) `toStream` — материализованная KTable → поток событий обновлений

```java
.toStream()
```

Оператор `aggregate` возвращает `KTable` — абстракцию «текущего состояния»: таблицу,
где каждый ключ представлен только своим последним значением. `toStream()` преобразует
`KTable` в поток событий изменений: каждое обновление агрегата (каждое добавление
нового спана в окно) порождает запись в потоке. Важный нюанс: при закрытии окна
Kafka Streams публикует **tombstone** — запись с `value = null`, сигнализирующую об
удалении из таблицы. Именно поэтому первым условием в следующем `filter` стоит
`agg != null`: без этой проверки попытка вызвать `agg.size()` на `null` привела бы
к `NullPointerException`, необработанному исключению в стрим-процессоре и его
остановке.

Дополнительно следует учитывать **eager evaluation**: `toStream()` производит запись
не только при закрытии окна, но при каждом обновлении состояния `KTable`. Это
означает, что финальный `filter` и `map` будут вызваны при каждом новом спане,
а не только по истечении 60 с. При высоком RPS это обеспечивает низкую задержку
обнаружения аномалии: если p99 превышает порог уже через 10–15 с после начала
деградации, алерт сгенерируется немедленно, не ожидая закрытия полного окна.

### (J) `filter` — двойное условие статистической надёжности

```java
.filter((wk, agg) ->
    agg != null
    && agg.size() >= lat.getMinSampleCount()
    && agg.p99() > lat.getP99ThresholdMs()
)
```

Оба содержательных условия обязательны и не заменяют друг друга:

**`agg.size() >= minSampleCount` (по умолчанию 10).** При малом числе наблюдений
оценка p99 T-Digest статистически ненадёжна: для n = 3 точек «99-й перцентиль»
вырождается в максимум выборки. Единственный выброс в 500 мс на фоне двух штатных
запросов в 50 мс вызвал бы алерт, хотя реальной деградации нет. Условие
`size >= 10` требует минимальной статистической базы: при 10 точках T-Digest даёт
робастную оценку хвоста. **Без этого условия**: каждый перезапуск сервиса или
временная пауза в трафике приводили бы к ложным срабатываниям на первых спанах.

**`agg.p99() > p99ThresholdMs` (по умолчанию 200 мс).** Основное условие
обнаружения аномалии. Проверяется только после гарантии статистической
надёжности. **Без этого условия**: при наличии достаточной выборки алерт
генерировался бы всегда — вне зависимости от реального значения p99.

### (K) `map` — формирование алерта и side effect

```java
.map((wk, agg) -> {
    AlertEvent event = new AlertEvent( ... );
    alertmanager.fire(event);              // side effect: HTTP POST к Alertmanager
    return KeyValue.pair(wk.key(), om.writeValueAsString(event));
})
```

Вызов `alertmanager.fire(event)` внутри `map` является **side effect** —
взаимодействием с внешней системой из «чистого» оператора преобразования потока.
В функциональной модели Kafka Streams это нарушение семантики `map`, однако здесь
оно обосновано. Kafka Streams гарантирует семантику **at-least-once** по умолчанию:
при сбое и повторной обработке одного и того же окна `alertmanager.fire(event)`
может быть вызван более одного раза. Alertmanager идемпотентен к повторным
оповещениям с одинаковыми лейблами: он дедуплицирует их по полю `fingerprint` и
не отправит несколько Slack-сообщений. Таким образом, at-least-once доставки
события в Alertmanager на практике эквивалентна exactly-once с точки зрения
конечного пользователя. Параллельная публикация события в `otel.alerts.pending`
через `.to(...)` обеспечивает дополнительную персистентность: если Alertmanager
временно недоступен, событие сохранится в Kafka и может быть обработано повторно.

### (L) `.to(ALERTS_TOPIC)` — выходной топик

```java
.to(ALERTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
```

Завершает топологию записью JSON-представления `AlertEvent` в топик
`otel.alerts.pending`. Оператор `.to(...)` возвращает `void` — это терминальная
операция, конец ветки DAG. Другие потребители (например, компонент мониторинга
дашборда или будущий aggregator-сервис) могут независимо читать из этого топика.

---

## Итоговая схема топологии

```
otel.spans
    │
    ▼ flatMapValues(parser::parse)
[SpanRecord stream, key=random]
    │
    ▼ filter(span != null && durationMs > 0)
[validated SpanRecord stream]
    │
    ▼ selectKey(span.serviceName)          ← repartition →
[SpanRecord stream, key=serviceName]
    │
    ▼ groupByKey + windowedBy(60s / 10s hopping)
[KGroupedStream → KTable<Windowed<String>, TDigestAgg>]
    │  (state store: latency-tdigest / RocksDB + changelog)
    ▼ toStream()
[KStream<Windowed<String>, TDigestAgg>]    ← eager, включая tombstones
    │
    ▼ filter(agg != null && size>=10 && p99>200ms)
[KStream<Windowed<String>, TDigestAgg>]    ← только реальные аномалии
    │
    ▼ map(agg → AlertEvent)  +  side effect: alertmanager.fire()
[KStream<String, String (JSON)>]
    │
    ▼ to(otel.alerts.pending)
```
