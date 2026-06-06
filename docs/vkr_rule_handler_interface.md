# Интерфейс RuleHandler и принцип открытости/закрытости

## Листинг

Листинг 1 — `RuleHandler.java`

```java
package com.example.monitoring.rule;

/**
 * Plugin interface for anomaly detection rules.
 * Add a new rule type by implementing this interface — no changes to the detector core.
 * Rule types: THRESHOLD (p99 latency), RATE (DLQ frequency), ABSENCE (heartbeat).
 */
public interface RuleHandler {
    /** Identifies this rule in AlertEvent.ruleType and Alertmanager labels. */
    String getRuleType();
}
```

---

## Описание

Интерфейс `RuleHandler` является точкой расширения («plugin interface») системы
обнаружения аномалий и непосредственно воплощает принцип открытости/закрытости из
набора SOLID: ядро детектора закрыто для модификации — оно не содержит условных
конструкций вида `if (ruleType == THRESHOLD) ... else if (ruleType == RATE) ...`, —
однако открыто для расширения через добавление новых реализаций интерфейса без
единого изменения в существующем коде. Чтобы ввести новый тип правила (например,
`ABSENCE` — отсутствие heartbeat-сигнала за заданный интервал), достаточно создать
класс, реализующий `RuleHandler`, зарегистрировать его как Spring-компонент и
определить логику генерации `AlertEvent` — инфраструктурный код маршрутизации и
сериализации при этом не затрагивается.

В текущей реализации интерфейс имеет два конкретных класса-реализатора. Класс
`LatencyTopology` (пакет `com.example.monitoring.streams`) представляет правило типа
**THRESHOLD**: он строит топологию Kafka Streams со скользящим окном, вычисляет p99
через T-Digest и генерирует `AlertEvent` при превышении порога `p99ThresholdMs`.
Класс `ConsumerLagMonitor` (пакет `com.example.monitoring.lag`) представляет правило
типа **RATE**: он опрашивает Prometheus API по расписанию, вычисляет частоту
поступления сообщений в DLQ-топик и публикует `AlertEvent` при превышении порога
`rateThreshold`.

Метод назван `getRuleType()`, а не, например, `getName()` или `getType()`, по двум
причинам. Во-первых, его возвращаемое значение используется в двух независимых
контекстах: оно копируется в поле `AlertEvent.ruleType` как часть доменной модели и
одновременно становится меткой (`label`) в теле HTTP-запроса к Alertmanager API, где
служит критерием маршрутизации. Специфичное имя `getRuleType` делает оба назначения
очевидными без обращения к документации. Во-вторых, в пакете `com.example.monitoring`
существуют и другие понятия с именем/типом (сервис, топик, severity), поэтому
уточняющий префикс `rule` устраняет неоднозначность при чтении кода и исключает
коллизии имён при рефакторинге.
