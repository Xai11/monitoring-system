# Алгоритмы мониторинга потребительского отставания и DLQ

Класс `ConsumerLagMonitor` реализует два независимых алгоритма мониторинга, работающих
по модели **опроса** (polling): вместо потоковой обработки Kafka Streams они
периодически запрашивают Prometheus HTTP API и формируют `AlertEvent` при
превышении пороговых значений. Оба алгоритма объединены общим вспомогательным
методом `queryPrometheus` и интерфейсом `MetricCallback`, что устраняет дублирование
HTTP-клиентского кода.

---

## Алгоритм `checkConsumerLag()` — мониторинг отставания потребителей

### Листинг

```java
/** Consumer lag — THRESHOLD rule for Kafka consumer groups. */
@Scheduled(fixedDelayString = "${monitoring.latency.slide-seconds:10}000")
public void checkConsumerLag() {
    long threshold = props.getConsumerLag().getThresholdMessages();
    String query = "sum(kafka_consumergroup_lag)by(topic,consumergroup)";
    queryPrometheus(query, "consumer_lag", (metric, value) -> {
        if (value > threshold) {
            String topic = metric.path("topic").asText("?");
            String group = metric.path("consumergroup").asText("?");
            AlertEvent event = new AlertEvent(
                group, "THRESHOLD", value, threshold, "warning",
                String.format("Consumer lag %s/%s = %.0f > %d", topic, group, value, threshold),
                null, Instant.now()
            );
            log.warn("[ALERT] ConsumerLag: {}/{} = {}", topic, group, (long) value);
            alertmanager.fire(event);
            publishToKafka(group, event);
        }
    });
}
```

### PromQL-запрос и имя метрики

Метод выполняет HTTP GET-запрос к Prometheus API:

```
GET http://prometheus:9090/api/v1/query
    ?query=sum(kafka_consumergroup_lag)by(topic%2Cconsumergroup)
```

Ответ представляет собой JSON-массив `data.result`, каждый элемент которого содержит
набор лейблов (`metric`) и мгновенное значение (`value`): число сообщений, на которое
отстаёт группа-потребитель в данном топике.

**Критически важное замечание о написании имени метрики.** Метрика называется
`kafka_consumergroup_lag` — без дополнительного подчёркивания между словами
`consumer` и `group`. Распространённая ошибка при написании PromQL-запросов —
использование варианта `kafka_consumer_group_lag` с лишним разделителем. Такой
запрос возвращает пустой результат без каких-либо сообщений об ошибке: Prometheus
просто не находит метрику с указанным именем, и `data.result` оказывается пустым
массивом. В результате мониторинг отставания молча прекращает работу, не генерируя
алертов даже при значительном накоплении необработанных сообщений. Метрика
`kafka_consumergroup_lag` экспортируется компонентом Kafka Exporter
(`danielqsj/kafka-exporter`) и следует его внутреннему соглашению об именовании,
которое отличается от соглашений библиотек JMX-экспортёра.

### Логика сравнения и формирование AlertEvent

Для каждой пары `(topic, consumergroup)` в результате PromQL-запроса численное
значение lag сравнивается с порогом `threshold` из конфигурации
(`consumer-lag.threshold-messages: 1000`). При превышении формируется `AlertEvent`
с полями: `service = consumergroup`, `ruleType = "THRESHOLD"`, `value = lag`,
`severity = "warning"`. Поле `traceId` устанавливается в `null`: consumer lag
является метрикой, агрегированной по топику и группе, а не свойством конкретного
сообщения — exemplar здесь неприменим. Аннотация `@JsonInclude(NON_NULL)` в
`AlertEvent` обеспечивает исключение поля из JSON-сериализации, не добавляя
строку `"traceId": null` в тело оповещения.

---

## Алгоритм `checkDlqRate()` — мониторинг частоты DLQ-сообщений

### Листинг

```java
/** DLQ rate — RATE rule: fires when DLQ topic has non-zero message rate. */
@Scheduled(fixedDelayString = "${monitoring.dlq.window-size-minutes:5}00000")
public void checkDlqRate() {
    double rateThreshold = props.getDlq().getRateThreshold();
    String query = "sum(rate(kafka_topic_partition_current_offset"
        + "{topic=~\".*[.-]dlq$|.*dead[.-]letter.*\"}[2m]))by(topic)";
    queryPrometheus(query, "dlq_rate", (metric, value) -> {
        if (value > rateThreshold) {
            String topic = metric.path("topic").asText("?");
            AlertEvent event = new AlertEvent(
                topic, getRuleType(), value, rateThreshold, "critical",
                String.format("DLQ %s rate=%.4f/s > threshold=%.4f", topic, value, rateThreshold),
                null, Instant.now()
            );
            log.error("[ALERT] DLQ rate: {} = {}/s", topic, value);
            alertmanager.fire(event);
            publishToKafka(topic, event);
        }
    });
}
```

### PromQL-запрос с регулярным выражением

Запрос к Prometheus:

```
GET http://prometheus:9090/api/v1/query
    ?query=sum(rate(kafka_topic_partition_current_offset
              {topic=~".*[.-]dlq$|.*dead[.-]letter.*"}[2m]))by(topic)
```

Селектор `topic=~"..."` применяет фильтрацию по регулярному выражению. Выражение
`.*[.-]dlq$|.*dead[.-]letter.*` охватывает два распространённых соглашения об
именовании DLQ-топиков:

- `.*[.-]dlq$` — любое имя, оканчивающееся на `.dlq` или `-dlq`
  (например, `orders.dlq`, `payments-dlq`);
- `.*dead[.-]letter.*` — любое имя, содержащее подстроку `dead.letter` или
  `dead-letter` (например, `orders.dead.letter`, `inventory-dead-letter-queue`).

Разделители `[.-]` (точка или дефис) включены в класс символов, поскольку Kafka
не допускает смешивания точек и дефисов в одном имени топика, однако разные
команды используют разные соглашения.

### Функция `rate()` применительно к offset-метрике

Метрика `kafka_topic_partition_current_offset` — монотонно возрастающий счётчик
(counter): он фиксирует смещение последнего записанного сообщения в партиции Kafka
и никогда не убывает (кроме случаев удаления топика). Функция PromQL `rate(metric[2m])`
вычисляет **среднюю скорость роста** счётчика за последние 2 минуты в единицах
«приращений в секунду». Применительно к `current_offset` это означает **среднее
число сообщений, записываемых в DLQ-топик за одну секунду** за последние 2 минуты.
Оператор `sum(...) by(topic)` суммирует значения по всем партициям топика, получая
суммарный входящий поток DLQ в сообщениях в секунду.

### Смысл порога `rateThreshold = 0.01`

Значение 0.01 сообщений в секунду соответствует **одному сообщению каждые 100 секунд**
(≈ 1,67 минуты). При пятиминутном окне мониторинга такой порог означает: допускается
не более 3 DLQ-сообщений за 5 минут до срабатывания алерта. Это очень низкий
порог, отражающий производственную стратегию «нулевой терпимости» к ошибкам
обработки: любое устойчивое появление сообщений в DLQ, кроме единичных переходных
сбоев, должно привлекать внимание дежурного. Уровень `severity: critical` (в отличие
от `warning` для consumer lag) подчёркивает, что попадание в DLQ означает потерю
данных, а не просто временное замедление обработки. При необходимости порог может
быть повышен через конфигурацию `monitoring.dlq.rate-threshold` без перекомпиляции.

---

## Интервалы опроса `@Scheduled`

```java
// checkConsumerLag: каждые slide-seconds (по умолчанию 10 с)
@Scheduled(fixedDelayString = "${monitoring.latency.slide-seconds:10}000")

// checkDlqRate: каждые window-size-minutes (по умолчанию 5 мин = 300 с)
@Scheduled(fixedDelayString = "${monitoring.dlq.window-size-minutes:5}00000")
```

Аннотация `@Scheduled(fixedDelayString = "...")` задаёт **фиксированную задержку**
(fixed delay) в миллисекундах: следующий запуск происходит через указанный интервал
после завершения предыдущего (а не через указанный интервал после начала, как при
`fixedRate`). Это предотвращает накопление параллельных вызовов при замедлении
Prometheus API.

Значения получаются конкатенацией параметра конфигурации с суффиксом нулей:
- `${monitoring.latency.slide-seconds:10}000` — значение в секундах умножается на
  1000 (дополнение тремя нулями), превращаясь в миллисекунды:
  `10` → `"10" + "000"` → `10000` мс = 10 с.
- `${monitoring.dlq.window-size-minutes:5}00000` — значение в минутах умножается
  на 100 000 (дополнение пятью нулями), превращаясь в миллисекунды:
  `5` → `"5" + "00000"` → `500000` мс = 500 с ≈ 8,3 мин.

Интервалы намеренно различаются. Consumer lag может нарасти стремительно — при
остановке потребителя отставание увеличивается пропорционально интенсивности
продюсеров, поэтому опрос каждые 10 с обеспечивает раннее обнаружение. DLQ-события
по природе своей редки: проверять их каждые 10 с означало бы генерировать
избыточную нагрузку на Prometheus при отсутствии значимых изменений. Пятиминутный
интервал соответствует интервалу агрегации `rate(...[2m])` в PromQL с двукратным
запасом, обеспечивая статистически значимое окно для расчёта частоты.

---

## Вспомогательная инфраструктура

```java
private void queryPrometheus(String promql, String ruleName, MetricCallback cb) {
    try {
        String url = prometheusUrl + "/api/v1/query?query=" +
            URLEncoder.encode(promql, StandardCharsets.UTF_8);
        String body = http.getForObject(url, String.class);
        JsonNode root = om.readTree(body);
        for (JsonNode result : root.path("data").path("result")) {
            double value = Double.parseDouble(result.path("value").get(1).asText("0"));
            cb.accept(result.path("metric"), value);
        }
    } catch (Exception e) {
        log.debug("Prometheus query '{}' failed: {}", ruleName, e.getMessage());
    }
}

@FunctionalInterface
interface MetricCallback {
    void accept(JsonNode metric, double value);
}
```

Метод `queryPrometheus` реализует шаблон «стратегия» через функциональный интерфейс
`MetricCallback`: логика HTTP-запроса и разбора JSON вынесена в один метод, тогда
как бизнес-логика сравнения с порогом остаётся в каждом конкретном алгоритме.
Prometheus Instant Query API возвращает значение метрики как второй элемент массива
`value`: `[timestamp, "значение_как_строка"]`. `Double.parseDouble()` используется
вместо `asDouble()` по той же причине, что и `Long.parseLong()` в парсере
OTLP-спанов: Prometheus возвращает числовые значения как строки JSON для сохранения
точности при передаче `NaN` и `+Inf`. При любой ошибке запроса или разбора метод
записывает сообщение на уровне `DEBUG` (не `WARN`): кратковременная недоступность
Prometheus не должна засорять журналы в production.
