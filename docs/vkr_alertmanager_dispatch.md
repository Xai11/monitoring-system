# Алгоритм диспетчеризации алертов

## Листинг методов `fire()` и `buildPayload()`

Листинг 1 — `AlertmanagerClient.java`

```java
public void fire(AlertEvent event) {
    try {
        String payload = buildPayload(event);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> resp = http.postForEntity(
            alertmanagerUrl + "/api/v2/alerts",          // (1) только v2
            new HttpEntity<>(payload, headers),
            String.class
        );
        log.info("Alert fired: service={} rule={} status={}",
            event.service(), event.ruleType(), resp.getStatusCode());
    } catch (Exception e) {
        log.warn("Failed to fire alert to Alertmanager: {}", e.getMessage());
    }
}

private String buildPayload(AlertEvent event) throws Exception {
    Map<String, Object> alert = Map.of(
        "labels", Map.of(
            "alertname", event.ruleType() + "_" + sanitize(event.service()), // (2)
            "service",   event.service(),
            "severity",  event.severity(),
            "ruletype",  event.ruleType()
        ),
        "annotations", Map.of(
            "summary",  nvl(event.summary()),
            "trace_id", nvl(event.traceId())             // (3) exemplar
        ),
        "startsAt", event.firedAt().toString(),
        "endsAt",   event.firedAt().plusSeconds(300).toString()  // (4) TTL 5 мин
    );
    return om.writeValueAsString(List.of(alert));        // (5) массив из одного алерта
}
```

---

## Эндпоинт `/api/v2/alerts` — почему не v1

Метод обращается строго к `/api/v2/alerts`. В версии Alertmanager 0.28.0 (февраль
2025 г.) эндпоинт `/api/v1/alerts` был окончательно удалён после длительного периода
депрекации, начавшегося с версии 0.20.0. Запрос к `/api/v1/alerts` на актуальной
версии Alertmanager возвращает `404 Not Found`, причём без какого-либо тела ответа,
описывающего причину. В логах это выглядит как «алерт отправлен, но статус 404» —
такая ситуация легко вводит в заблуждение при отладке: код HTTP 2xx не получен, но
и явная ошибка парсинга не возникает. Использование `/api/v2/alerts` является
единственным поддерживаемым интерфейсом для программной отправки алертов начиная
с актуальных выпусков.

Тело запроса — JSON-массив объектов-алертов. Несмотря на то что в данной реализации
массив всегда содержит один элемент (строка `List.of(alert)`), формат массива
соответствует спецификации API и позволяет в будущем объединять несколько алертов
в один HTTP-запрос без изменения структуры.

---

## Структура payload: labels и annotations

Объект-алерт состоит из четырёх секций: `labels`, `annotations`, `startsAt`, `endsAt`.

### Labels — идентификатор алерта

```json
"labels": {
    "alertname": "THRESHOLD_order_service",
    "service":   "order-service",
    "severity":  "warning",
    "ruletype":  "THRESHOLD"
}
```

В модели данных Alertmanager **labels** — это не просто метаданные, а
**первичный ключ** алерта: уникальность определяется хэшем (fingerprint) всего
набора лейблов. Два алерта с одинаковыми лейблами считаются одним и тем же
инцидентом вне зависимости от времени поступления.

Поле `alertname` формируется конкатенацией `ruleType` и санированного имени сервиса:
`"THRESHOLD_order_service"`. Метод `sanitize()` заменяет дефисы и точки
подчёркиваниями — это обязательное требование Alertmanager, поскольку значения
лейблов используются как имена метрик в Prometheus-модели данных самого
Alertmanager, а дефисы и точки недопустимы в именах меток Prometheus.

### Annotations — контекст для дежурного

```json
"annotations": {
    "summary":  "p99=312ms > threshold=200ms (n=145)",
    "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736"
}
```

`annotations` не влияют на идентичность алерта и не участвуют в дедупликации.
Они передаются в шаблон Alertmanager-уведомления и отображаются в теле
Slack-сообщения. Поле `summary` содержит человекочитаемую строку с фактическим
значением p99, порогом и объёмом выборки — именно этот текст дежурный видит
первым при получении уведомления.

---

## TTL алерта: логика `endsAt`

```java
"endsAt", event.firedAt().plusSeconds(300).toString()
```

Alertmanager поддерживает алерт в состоянии `firing` до тех пор, пока не наступит
одно из двух событий: либо приходит новый экземпляр алерта с теми же лейблами
(«продление» TTL), либо истекает время `endsAt`. Значение `endsAt = firedAt + 300 с`
устанавливает **TTL в 5 минут**.

Это значение выбрано с учётом частоты повторных срабатываний. Топология с
hopping-окном 60 с / шаг 10 с при непрерывной деградации генерирует новый алерт
каждые 10 с (каждый раз, когда `toStream()` публикует обновлённый агрегат,
превышающий порог). При такой частоте TTL в 5 минут обеспечивает:

- **Автоматическое завершение** (`resolved`): если деградация прекратилась и
  алерты перестали приходить, Alertmanager переведёт алерт в `resolved` через
  5 минут — без необходимости явно посылать «закрывающее» событие.
- **Отсутствие ложного resolved** при кратковременных паузах: если между двумя
  срабатываниями прошло 11–15 с (например, один цикл не попал под порог), TTL
  300 с гарантирует, что алерт не закроется и не откроется заново — дежурный не
  получит флуд сообщений «resolved / firing / resolved / firing».

---

## Дедупликация в Alertmanager

Alertmanager вычисляет `fingerprint` каждого поступившего алерта как хэш SHA-256
от отсортированного набора его лейблов. Два алерта считаются идентичными (и схлопываются
в один активный инцидент), если их fingerprint совпадает. При этом `annotations`
обновляются значением последнего поступившего алерта, а `startsAt` остаётся
временем первого срабатывания.

Рассмотрим конкретный сценарий с hopping-окном. За одну минуту непрерывной деградации
топология генерирует **6 алертов** (один каждые 10 с — по числу шагов окна 60/10).
Каждый из них имеет одинаковый набор лейблов:

```
alertname=THRESHOLD_order_service, service=order-service,
severity=warning, ruletype=THRESHOLD
```

Следовательно, все 6 алертов имеют одинаковый fingerprint. Alertmanager
регистрирует первый как новый активный инцидент, остальные пять — продлевают
его TTL и обновляют `annotations.summary` и `annotations.trace_id` последним
значением. Дежурный получает **одно** уведомление в Slack при первом срабатывании
и одно — при переходе в `resolved`, а не шесть уведомлений подряд.

---

## Поле `trace_id` и навигация Grafana → Jaeger

Поле `trace_id` в `annotations` передаётся в шаблон Alertmanager-уведомления.
При настройке Slack-шаблона значение подставляется в ссылку вида:

```
http://jaeger:16686/trace/{{ .Annotations.trace_id }}
```

Это позволяет дежурному перейти непосредственно к конкретному трейсу одним
кликом из Slack-сообщения, минуя поиск по времени в интерфейсе Jaeger.

Дополнительно Grafana поддерживает «Derived Fields» в источнике данных Loki:
если в строке журнала присутствует поле `trace_id`, Grafana автоматически
рендерит его как гиперссылку на Jaeger. Поскольку OTel Java-агент добавляет
`trace_id` в каждую строку журнала через MDC (Mapped Diagnostic Context), поле
`trace_id` в `AlertEvent` и `trace_id` в журналах сервиса в Loki ссылаются на
**один и тот же трейс** — обеспечивается единая цепочка навигации:
алерт → журнал → трейс → отдельный спан.
