# Раздел F — Эксплуатация и наблюдаемость

> Аудит кодовой базы `github.com/ikermy/BFF` (ветка `codebase-analysis`, HEAD `e3b3be7 Kafka DLQ`).
> Метод: чтение кода + запуск `go build` / `go vet` / `go test` + детерминированные пробы (`httptest`, чтение Prometheus-счётчиков) + чтение исходников `segmentio/kafka-go` из модкэша.
> Пробы после подтверждения удалены, в репозиторий не коммитились.

---

## Оглавление

| ID | Название | Критичность | Статус |
|----|----------|-------------|--------|
| F-01 | HEAD не компилируется: забытый аргумент `NewConsumer` | **Блокер** | Подтверждён |
| F-02 | `/health` — статическая заглушка, не проверяет зависимости | **Критичный** | Подтверждён |
| F-03 | Нет разделения liveness / readiness | Высокий | Подтверждён |
| F-04 | Полностью отсутствует трассировка (`traceID` не пробрасывается) | **Критичный** | Подтверждён |
| F-05 | Worker не отдаёт метрики: Prometheus его не видит | **Критичный** | Подтверждён |
| F-06 | Нет метрик исходов саги и провала компенсации | **Критичный** | Подтверждён |
| F-07 | `bulk/wake` возвращает недостоверный `pendingMessages` | Высокий | Подтверждён |
| F-08 | `PendingCount()` использует деструктивный `Stats()` | Высокий | Подтверждён |
| F-09 | Нет метрик Billing / AI / History / Kafka / Redis | Высокий | Подтверждён |
| F-10 | Нет алертов (`alerting_rules` отсутствуют) | Высокий | Подтверждён |
| F-11 | Grafana-дашборд использует 2 метрики из 5 | Средний | Подтверждён |
| F-12 | У `worker` нет healthcheck в compose | Средний | Подтверждён |
| F-13 | Логи без структуры и уровней (`log.Printf`) | Средний | Подтверждён |
| F-14 | `MetricsMiddleware` не пишет латентность HTTP | Средний | Подтверждён |
| F-15 | Метрика ошибок схлопывает 4xx и 5xx в `error` | Средний | Подтверждён |
| F-16 | Артефакты покрытия закоммичены в репозиторий | Низкий | Подтверждён |
| F-17 | Дубль `internal/app/configs/` из-за относительных путей | Низкий | Подтверждён |
| F-18 | README ссылается на несуществующий `deploy/` | Низкий | Подтверждён |
| F-19 | `Makefile` не имеет `build` / `lint` / `vet` / `race` | Низкий | Подтверждён |
| F-20 | Нет CI-конфигурации | Средний | Подтверждён |
| F-21 | Cardinality метрик — безопасна (опровергнутая гипотеза) | — | **Опровергнут** |

---

## F-01. HEAD не компилируется: забытый аргумент `NewConsumer`

**Суть.** Последний коммит расширил сигнатуру `NewConsumer`, добавив DLQ-publisher, обновил вызов в `worker_app.go`, но пропустил вызов в `api_app.go`. Ветка не собирается.

**Место.**
- `internal/app/api_app.go:219` — вызов с 3 аргументами
- `internal/adapters/kafka/consumer.go:31` — сигнатура с 4 параметрами
- `internal/app/worker_app.go:133` — корректно обновлённый вызов

**Подробное описание.**

Фактический вывод `go build ./...`:

```
# github.com/ikermy/BFF/internal/app
internal/app/api_app.go:219:81: not enough arguments in call to kafkaadapter.NewConsumer
	have (string, string, func(ctx context.Context, msg domain.BulkJobMessage) error)
	want (string, string, kafka.MessageHandler, kafka.Publisher)
```

`go vet ./...` падает с той же ошибкой. `go test ./...` даёт:

```
FAIL github.com/ikermy/BFF/cmd/api      [build failed]
FAIL github.com/ikermy/BFF/cmd/worker   [build failed]
FAIL github.com/ikermy/BFF/internal/app [build failed]
```

Остальные 13 пакетов зелёные — то есть дефект локализован в сборке приложения, а не в логике. Но следствия тяжёлые:

1. **Ни один бинарь не собирается.** `Dockerfile` строит `./cmd/api` и `./cmd/worker` — оба падают. `make dev-up` / `prod-up` не поднимут стек.
2. **`internal/app` — единственный пакет, где происходит проводка зависимостей.** Он же не покрыт тестами, из-за чего расхождение не поймали.
3. **Все фиксы из разделов A–E невозможно проверить**, пока сборка красная.

Трассировка причины: DLQ-функциональность добавлялась в `consumer.go` (`publishDLQ` на строках 88-108), сигнатура стала `NewConsumer(brokers, groupID, handler, dlqPublisher)`. В `worker_app.go:133` передан `kafkaProducer`. В `api_app.go:219` — нет. Компилятор не поймал это на CI, потому что CI отсутствует (см. F-20).

**Предлагаемый багфикс.**

```go
// internal/app/api_app.go:219
bulkConsumer = kafkaadapter.NewConsumer(
    cfg.Kafka.Brokers,
    cfg.Kafka.GroupID,
    bulkHandler.Handle,
    kafkaProducer, // DLQ publisher (может быть nil — publishDLQ это проверяет)
)
```

Важно: `kafkaProducer` в `api_app.go` может быть `nil`, если `KAFKA_BROKERS` не задан. `publishDLQ` начинается с `if c.dlqPublisher == nil { return }`, но передача типизированного `nil`-указателя в интерфейс даст non-nil интерфейс. Поэтому:

```go
var dlq kafkaadapter.Publisher
if kafkaProducer != nil {
    dlq = kafkaProducer
}
bulkConsumer = kafkaadapter.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.GroupID, bulkHandler.Handle, dlq)
```

Плюс закрыть корневую причину — добавить `internal/app` в тесты хотя бы smoke-проверкой:

```go
// internal/app/api_app_test.go
func TestBuildAPIApp_Smoke(t *testing.T) {
    t.Setenv("KAFKA_BROKERS", "")      // mock-путь
    cfg := config.Load()
    app := BuildAPIAppWithContext(context.Background(), cfg)
    if app == nil {
        t.Fatal("BuildAPIAppWithContext returned nil")
    }
    app.Close()
}
```

**Комментарий.** Это единственный дефект уровня «блокер» во всём аудите и правится одной строкой. Но он показателен: пакет проводки зависимостей — самое хрупкое место в ручном DI, и именно он не покрыт ни тестами, ни CI. Один smoke-тест на `BuildAPIApp` + `BuildWorkerApp` предотвратил бы весь класс таких ошибок. Исправлять нужно первым, до всех остальных фиксов — иначе их нельзя верифицировать.

---

## F-02. `/health` — статическая заглушка, не проверяет зависимости

**Суть.** Хендлер `/health` безусловно возвращает `200 {"status":"ok"}`. Он не проверяет ни Billing, ни BarcodeGen, ни Redis, ни Kafka. Контейнер считается здоровым, даже когда сервис не способен обслужить ни один запрос.

**Место.**
- `internal/transport/http/gin/router.go:37-39` — хендлер
- `Dockerfile:22-23` — `HEALTHCHECK` бьёт в этот эндпоинт
- `docker-compose.yml:47-52` — healthcheck сервиса `bff`

**Подробное описание.**

Код целиком:

```go
r.GET("/health", func(c *gin.Context) {
    c.JSON(http.StatusOK, gin.H{"status": "ok"})
})
```

Проба подтверждает:

```
PROBE F-02: status=200 body={"status":"ok"}
PROBE F-02: no Billing/BarcodeGen/Redis/Kafka check in handler body -> always 200
```

Сценарий отказа, который healthcheck не заметит:

1. Redis упал. Идемпотентность перестаёт работать: `Reserve` возвращает ошибку, которая по **B-08** маскируется в `409 REQUEST_IN_FLIGHT`. Все запросы на генерацию отбиваются.
2. `/health` возвращает `200`.
3. Docker/K8s считает контейнер здоровым, трафик продолжает идти.
4. Все клиенты получают `409`. Ни один алерт не срабатывает (алертов нет вообще — F-10).

То же для Billing (все генерации падают с `BILLING_ERROR`), для BarcodeGen (все падают после 3 ретраев по 94 с — см. A-01), для Kafka (события не публикуются).

Отдельно: healthcheck в `Dockerfile` использует `wget -qO-`, который считает успехом любой 2xx. Так как эндпоинт всегда 2xx, healthcheck вырождается в проверку «процесс слушает порт» — что уже покрывается TCP-пробой.

**Предлагаемый багфикс.**

Разделить на две проверки (см. также F-03) и в readiness реально пинговать зависимости:

```go
// internal/ports/ports.go
type HealthChecker interface {
    Name() string
    Check(ctx context.Context) error
}

// internal/transport/http/gin/health.go
func HealthHandler(checkers []ports.HealthChecker) gin.HandlerFunc {
    return func(c *gin.Context) {
        ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
        defer cancel()

        results := make(map[string]string, len(checkers))
        degraded := false

        // Проверки параллельно, чтобы суммарно уложиться в 2 с
        var mu sync.Mutex
        var wg sync.WaitGroup
        for _, ch := range checkers {
            wg.Add(1)
            go func(ch ports.HealthChecker) {
                defer wg.Done()
                err := ch.Check(ctx)
                mu.Lock()
                defer mu.Unlock()
                if err != nil {
                    results[ch.Name()] = "error: " + err.Error()
                    degraded = true
                } else {
                    results[ch.Name()] = "ok"
                }
            }(ch)
        }
        wg.Wait()

        status := http.StatusOK
        overall := "ok"
        if degraded {
            status = http.StatusServiceUnavailable
            overall = "degraded"
        }
        c.JSON(status, gin.H{"status": overall, "checks": results})
    }
}
```

Реализация чекера для Redis:

```go
func (s *RedisStore) Name() string { return "redis" }
func (s *RedisStore) Check(ctx context.Context) error {
    return s.client.Ping(ctx).Err()
}
```

Для HTTP-зависимостей — не полноценный вызов, а либо их `/health`, либо состояние circuit breaker'а, чтобы не создавать каскад нагрузки при каждом опросе.

**Комментарий.** Критичность высокая именно из-за связки с fail-open поведением из раздела E: сервис умеет молча деградировать (mock-адаптеры, `anonymous`-пользователь, Redis-in-memory), а `/health` устроен так, что деградацию невозможно обнаружить. Это два дефекта, усиливающие друг друга: один создаёт тихий отказ, другой гарантирует, что о нём не узнают.

---

## F-03. Нет разделения liveness / readiness

**Суть.** Единственный `/health` используется и как liveness (жив ли процесс), и как readiness (готов ли принимать трафик). Если сделать его зависимым от внешних сервисов (F-02), orchestrator начнёт перезапускать здоровый под из-за недоступности Billing.

**Место.**
- `internal/transport/http/gin/router.go:37-40` — один эндпоинт
- `Dockerfile:22-23`, `docker-compose.yml:47-52` — единственная проба
- `internal/transport/http/gin/middleware.go:25-26` — исключение из maintenance-режима

**Подробное описание.**

Сейчас есть только `/health`. Это создаёт ловушку при исправлении F-02: если добавить в него проверку Billing и вернуть `503`, то в Kubernetes с `livenessProbe: /health` при падении Billing начнётся **массовый рестарт всех подов BFF**, хотя сам BFF полностью работоспособен. Каскад: Billing лежит 5 минут → все поды BFF в `CrashLoopBackOff` → после восстановления Billing BFF ещё несколько минут не поднимается.

Правильное разделение:

| Проба | Что проверяет | Реакция orchestrator |
|-------|---------------|----------------------|
| liveness | Процесс не в deadlock, event loop живёт | Рестарт контейнера |
| readiness | Все зависимости доступны | Убрать из балансировки |
| startup | Конфиги загружены, соединения установлены | Дать время на старт |

Отдельная деталь: в `MaintenanceModeMiddleware` (`middleware.go:25-26`) `/health` и `/metrics` исключены из maintenance-режима. Это верно для liveness, но **неверно для readiness**: в maintenance-режиме сервис как раз не должен получать трафик, то есть readiness обязан отдавать `503`.

**Предлагаемый багфикс.**

```go
// internal/transport/http/gin/router.go

// /livez — только процесс. Никаких внешних вызовов.
r.GET("/livez", func(c *gin.Context) {
    c.JSON(http.StatusOK, gin.H{"status": "alive"})
})

// /readyz — зависимости + maintenance. Управляет трафиком.
r.GET("/readyz", ReadyHandler(checkers, maintenanceMode))

// /health — оставить как алиас на /livez для обратной совместимости
// (Dockerfile HEALTHCHECK и старые дашборды на него ссылаются)
r.GET("/health", func(c *gin.Context) {
    c.JSON(http.StatusOK, gin.H{"status": "ok"})
})
```

```go
func ReadyHandler(checkers []ports.HealthChecker, maintenance bool) gin.HandlerFunc {
    return func(c *gin.Context) {
        if maintenance {
            c.JSON(http.StatusServiceUnavailable, gin.H{"status": "maintenance"})
            return
        }
        // ... проверки зависимостей как в F-02
    }
}
```

Обновить `Dockerfile`:

```dockerfile
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:8080/livez || exit 1
```

И `docker-compose.yml` для `bff`:

```yaml
healthcheck:
  test: ["CMD", "wget", "-qO-", "http://localhost:8080/livez"]
```

**Комментарий.** Этот пункт нужно исправлять **вместе с F-02, а не после**. Если сначала добавить проверки зависимостей в единственный `/health`, получится регресс хуже исходной проблемы — самоподдерживающийся каскад рестартов. Порядок: сначала развести `/livez` и `/readyz`, потом наполнять `/readyz` проверками.

---

## F-04. Полностью отсутствует трассировка (`traceID` не пробрасывается)

**Суть.** В проекте нет ни OpenTelemetry, ни собственного request-ID. Невозможно связать HTTP-запрос клиента с логами BFF, вызовами Billing/BarcodeGen/AI и событиями Kafka. При разборе инцидента с деньгами нет способа восстановить цепочку.

**Место.**
- Весь `internal/` — ни одного упоминания `traceID`, `X-Request-Id`, `otel`
- `internal/domain/bulk_job.go:27` — единственное поле `CorrelationID`, **нигде не используется**
- `internal/transport/http/gin/middleware.go` — нет middleware генерации ID
- `internal/adapters/*/http_client.go` — исходящие запросы без корреляционных заголовков

**Подробное описание.**

Grep по всей кодовой базе:

```
=== traceID / request id ===
internal/domain/bulk_job.go:27:	CorrelationID string `json:"correlationId"`
=== otel ===
(none if empty)
```

То есть: поле `CorrelationID` объявлено в структуре bulk-сообщения, но не заполняется при публикации и не читается при обработке. Проба подтверждает, что клиентский `X-Request-Id` игнорируется:

```
PROBE F-04: sent X-Request-Id=client-supplied-trace-abc123
PROBE F-04: response headers = map[Content-Type:[application/json; charset=utf-8]]
PROBE F-04: X-Request-Id in response = "" (пусто = не пробрасывается)
PROBE F-04: no traceId in response body -> клиент не может сослаться на запрос в поддержке
```

Практические следствия. Возьмём реальный инцидент из раздела A: **A-01** (деньги заблокированы навсегда, потому что `Release` вызван на отменённом контексте). Разбор без трассировки:

1. Пользователь жалуется: «списали деньги, баркод не пришёл».
2. В логах BFF (`log.Printf`, F-13) есть строки вида `barcode generation failed: ...`, но без ID запроса и без `userID` — их невозможно отфильтровать по этому пользователю.
3. В Billing есть заблокированная сумма с каким-то `sagaID`. Найти его в логах BFF можно только полнотекстовым поиском по времени.
4. Связать это с конкретным вызовом BarcodeGen нельзя вообще — BFF не передаёт корреляционный заголовок вниз.

При bulk-обработке ситуация ещё хуже: одно сообщение Kafka порождает N генераций, каждая со своими вызовами Billing/BarcodeGen/AI, и все они анонимны.

Дополнительно: `sagaID` фактически мог бы служить корреляционным ID, но он (а) генерируется в usecase уже после валидации, (б) не попадает в логи адаптеров, (в) для edit детерминирован и коллизионен (B-09).

**Предлагаемый багфикс.**

Минимальный вариант без внешних зависимостей — middleware + проброс через `context`:

```go
// internal/transport/http/gin/middleware.go
type ctxKey string

const CtxKeyTraceID ctxKey = "traceID"
const HeaderTraceID = "X-Request-Id"

func TraceIDMiddleware() gin.HandlerFunc {
    return func(c *gin.Context) {
        traceID := c.GetHeader(HeaderTraceID)
        if traceID == "" {
            traceID = uuid.NewString()
        }
        // в ответ — чтобы клиент мог сослаться на него в поддержке
        c.Header(HeaderTraceID, traceID)
        // в context — чтобы usecase и адаптеры видели
        ctx := context.WithValue(c.Request.Context(), CtxKeyTraceID, traceID)
        c.Request = c.Request.WithContext(ctx)
        c.Next()
    }
}

func TraceIDFrom(ctx context.Context) string {
    if v, ok := ctx.Value(CtxKeyTraceID).(string); ok {
        return v
    }
    return ""
}
```

Проброс в исходящие HTTP-вызовы — в общем помощнике `post()`:

```go
// internal/adapters/barcodegen/http_client.go, в post()
if tid := gintransport.TraceIDFrom(ctx); tid != "" {
    req.Header.Set("X-Request-Id", tid)
}
```

(лучше вынести `TraceIDFrom` в отдельный пакет `internal/tracing`, чтобы адаптеры не импортировали transport — см. D-раздел про утечку слоёв)

Проброс в Kafka — заполнить существующее поле:

```go
msg := domain.BulkJobMessage{
    ...
    CorrelationID: tracing.From(ctx),
}
```

и на стороне консьюмера положить его обратно в контекст:

```go
func (h *BulkJobHandler) Handle(ctx context.Context, msg domain.BulkJobMessage) error {
    ctx = tracing.With(ctx, msg.CorrelationID)
    ...
}
```

Полноценный вариант — OpenTelemetry с `otelgin` и `otelhttp`, что даст ещё и спаны с латентностью по каждому downstream-вызову. Учитывая, что Prometheus/Loki/Grafana уже развёрнуты, добавление Tempo/Jaeger в `docker-compose.observability.yml` логично.

**Комментарий.** Для сервиса-оркестратора, который распоряжается деньгами и делает 4+ исходящих вызова на один входящий запрос, отсутствие корреляции — самый дорогой пробел в наблюдаемости. Он не вызывает отказов сам по себе, но умножает время разбора каждого инцидента из разделов A и B. Показательно, что поле `CorrelationID` уже спроектировано — то есть замысел был, реализация не дошла.

---

## F-05. Worker не отдаёт метрики: Prometheus его не видит

**Суть.** `cmd/worker` не поднимает HTTP-сервер и не экспонирует `/metrics`. Prometheus сконфигурирован только на `bff:8080`. Все метрики, инкрементируемые внутри worker (генерация, BarcodeGen, partial success), никуда не попадают.

**Место.**
- `cmd/worker/main.go` — только `worker.Run(ctx)`, HTTP-сервера нет
- `monitoring/prometheus.yml:6-10` — единственный target `bff:8080`
- `docker-compose.yml:53-73` — у сервиса `worker` не проброшен ни один порт
- `internal/metrics/metrics.go` — метрики регистрируются при импорте пакета, но экспортировать их некому

**Подробное описание.**

`cmd/worker/main.go` целиком:

```go
func main() {
	cfg := config.Load()
	worker := app.BuildWorkerApp(cfg)
	defer worker.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := worker.Run(ctx); err != nil {
		log.Fatalf("worker stopped with error: %v", err)
	}
}
```

Ни `promhttp.Handler()`, ни `http.ListenAndServe`. `monitoring/prometheus.yml`:

```yaml
scrape_configs:
  - job_name: barcode-bff
    metrics_path: /metrics
    static_configs:
      - targets:
          - bff:8080
```

Теперь ключевое: worker выполняет **тот же** `GenerateUseCase`, что и API (`worker_app.go` собирает его через тот же билдер). Значит внутри worker инкрементируются:

- `metrics.BarcodeGenCallsTotal` (`generate_usecase.go:429,434,437,446`)
- `metrics.PartialSuccessTotal` (`generate_usecase.go:300`)
- `metrics.ChainExecutionDurationMs` (`chain_executor.go:38`)

И всё это теряется. Практический эффект: **массовая bulk-генерация полностью невидима в мониторинге**. Дашборд «BarcodeGen calls rate» показывает только синхронный трафик через API. Если worker начнёт отбивать все задачи (BarcodeGen лежит, DLQ переполняется), на графиках не изменится ничего.

Дополнительно теряются `go_*` метрики рантайма worker'а: goroutines, heap, GC. То есть утечку горутин или память в worker нельзя обнаружить.

**Предлагаемый багфикс.**

Поднять в worker минимальный HTTP-сервер только для метрик и проб:

```go
// cmd/worker/main.go
func main() {
	cfg := config.Load()
	worker := app.BuildWorkerApp(cfg)
	defer worker.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Метрики и liveness worker'а (F-05, F-12)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"alive"}`))
	})
	metricsSrv := &http.Server{
		Addr:              cfg.WorkerMetricsPort, // напр. ":8081"
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("worker metrics server: %v", err)
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsSrv.Shutdown(shutdownCtx)
	}()

	if err := worker.Run(ctx); err != nil {
		log.Fatalf("worker stopped with error: %v", err)
	}
}
```

`monitoring/prometheus.yml`:

```yaml
scrape_configs:
  - job_name: barcode-bff-api
    metrics_path: /metrics
    static_configs:
      - targets: ["bff:8080"]
        labels:
          component: api

  - job_name: barcode-bff-worker
    metrics_path: /metrics
    static_configs:
      - targets: ["worker:8081"]
        labels:
          component: worker
```

`docker-compose.yml`, сервис `worker`:

```yaml
    environment:
      WORKER_METRICS_PORT: "8081"
    expose:
      - "8081"
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:8081/livez"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 10s
```

Важно: так как API и worker используют один и тот же пакет `metrics`, после добавления скрейпа worker'а метрики начнут приходить из двух источников с одинаковыми именами. Метка `component` (или стандартные `job`/`instance`) обязательна, иначе в дашбордах API и worker сложатся в одну кривую. Существующие запросы вида `sum(increase(bff_requests_total[5m]))` нужно уточнить до `sum by (job) (...)`.

**Комментарий.** Дефект структурный: асинхронный путь обработки (который тратит те же деньги и вызывает те же внешние сервисы) полностью выпал из мониторинга. При этом Loki через promtail собирает логи **обоих** контейнеров по docker-сокету, то есть логи worker'а видны, а метрики — нет. Это создаёт обманчивое ощущение покрытия.

---

## F-06. Нет метрик исходов саги и провала компенсации

**Суть.** Всего 5 метрик, и ни одна не отражает исход распределённой транзакции. Нет счётчиков `Block`/`Capture`/`Release`, нет счётчика **провала компенсации** — то есть именно те инциденты, где зависают деньги пользователей (A-01, A-04), не наблюдаемы вообще.

**Место.**
- `internal/metrics/metrics.go` — все 5 метрик
- `internal/usecase/generate_usecase.go:300` — единственная метрика саги (`PartialSuccessTotal`)
- `internal/usecase/generate_usecase.go` — вызовы `Block`/`Capture`/`Release` без метрик
- `internal/usecase/edit_usecase.go` — free-edit путь без метрик

**Подробное описание.**

Полный список существующих метрик:

| Метрика | Тип | Метки | Где инкрементируется |
|---------|-----|-------|----------------------|
| `bff_requests_total` | CounterVec | `endpoint`, `status` | `middleware.go:45` |
| `bff_partial_success_total` | Counter | — | `generate_usecase.go:300` |
| `bff_chain_execution_duration_ms` | Histogram | — | `chain_executor.go:38` |
| `bff_barcodegen_calls_total` | CounterVec | `status` | `generate_usecase.go:429,434,437,446` |
| `bff_duplicate_requests_total` | Counter | — | `idempotency_middleware.go:77,97` |

Чего нет:

1. **Исход саги.** Нельзя построить график `success / partial / failed`. Есть только `partial_success_total` — абсолютный счётчик без знаменателя.
2. **Провал компенсации.** Самое важное. Сценарии A-01 и A-04 приводят к тому, что `Release` не выполняется и деньги остаются заблокированными. Сейчас это только строка в логе. Нужен счётчик, на который можно поставить алерт «любое значение > 0 — инцидент».
3. **Billing-операции.** Нет `bff_billing_calls_total{operation,status}` — нельзя увидеть, что `Capture` начал массово падать.
4. **Free-edit.** Нет метрик на `CheckFreeEdit` / платное редактирование, хотя там гонка A-02 приводит к бесплатным списаниям.
5. **Идемпотентность.** Есть только `duplicate_requests_total`. Нет счётчиков `in_flight` (409), провала `Reserve`, переполнения лимита 1 МБ (B-07 — молча не кэширует).
6. **DLQ.** Нет счётчика сообщений, ушедших в DLQ.

**Предлагаемый багфикс.**

```go
// internal/metrics/metrics.go

// SagaOutcomeTotal — исход саги генерации. Labels: outcome ("success"/"partial"/"failed")
SagaOutcomeTotal = promauto.NewCounterVec(
    prometheus.CounterOpts{
        Name: "bff_saga_outcome_total",
        Help: "Total number of generation sagas by final outcome.",
    },
    []string{"outcome"},
)

// CompensationFailuresTotal — провал компенсации: деньги остались заблокированными.
// КРИТИЧНО: любое значение > 0 требует ручного разбора.
// Labels: operation ("release"/"capture"), reason ("ctx_canceled"/"billing_error")
CompensationFailuresTotal = promauto.NewCounterVec(
    prometheus.CounterOpts{
        Name: "bff_compensation_failures_total",
        Help: "Billing compensation failures leaving funds blocked. Requires manual reconciliation.",
    },
    []string{"operation", "reason"},
)

// BillingCallsTotal — вызовы Billing. Labels: operation ("quote"/"block"/"capture"/"release"), status
BillingCallsTotal = promauto.NewCounterVec(
    prometheus.CounterOpts{
        Name: "bff_billing_calls_total",
        Help: "Total number of calls to Billing service.",
    },
    []string{"operation", "status"},
)

// BlockedUnitsGauge — units, заблокированные прямо сейчас (для сверки с Billing).
BlockedUnitsGauge = promauto.NewGauge(
    prometheus.GaugeOpts{
        Name: "bff_blocked_units_current",
        Help: "Units currently blocked by in-flight sagas.",
    },
)

// DLQMessagesTotal — сообщения, отправленные в DLQ. Labels: topic, reason
DLQMessagesTotal = promauto.NewCounterVec(
    prometheus.CounterOpts{
        Name: "bff_dlq_messages_total",
        Help: "Messages published to dead letter queue.",
    },
    []string{"topic", "reason"},
)

// IdempotencyEventsTotal — события идемпотентности.
// Labels: event ("hit"/"in_flight"/"reserve_error"/"body_too_large")
IdempotencyEventsTotal = promauto.NewCounterVec(
    prometheus.CounterOpts{
        Name: "bff_idempotency_events_total",
        Help: "Idempotency middleware events.",
    },
    []string{"event"},
)
```

Инструментирование компенсации (совместно с фиксом A-01):

```go
// generate_usecase.go, компенсирующий Release
if err := u.billing.Release(compensationCtx, sagaID, failedUnits); err != nil {
    reason := "billing_error"
    if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
        reason = "ctx_canceled"
    }
    metrics.CompensationFailuresTotal.WithLabelValues("release", reason).Inc()
    log.Printf("CRITICAL: release failed, funds remain blocked: saga=%s units=%d err=%v",
        sagaID, failedUnits, err)
}
```

**Комментарий.** Прямое продолжение разделов A и B: там найдены пути, на которых деньги зависают или списываются дважды, и ни один из них сейчас не наблюдаем. `bff_compensation_failures_total` — самая важная из предложенных метрик: это единственный способ узнать, что в Billing образовалось расхождение, не дожидаясь жалобы пользователя. Ставить её нужно одновременно с фиксом A-01, иначе непонятно, помог ли фикс.

---

## F-07. `bulk/wake` возвращает недостоверный `pendingMessages`

**Суть.** Эндпоинт задуман как проверка «BFF читает Kafka», но в API-процессе консьюмер **никогда не запускается**. `PendingCount()` читает лаг у reader'а, который не сделал ни одного fetch, и почти всегда возвращает 0. Эндпоинт систематически отвечает «всё в порядке» независимо от реальности.

**Место.**
- `internal/transport/http/gin/api_handler.go:156-163` — хендлер
- `internal/app/api_app.go:216-223` — создание консьюмера
- `internal/app/api_app.go:34-36` — `APIApp.Run` запускает только HTTP-сервер
- `internal/adapters/kafka/consumer.go:110-117` — `PendingCount()`
- `internal/adapters/kafka/consumer.go:46` — `Start()`, который не вызывается

**Подробное описание.**

Хендлер:

```go
func (h *APIHandler) BulkWake(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":          "awake",
		"pendingMessages": h.consumer.PendingCount(),
	})
}
```

`APIApp.Run` целиком:

```go
func (a *APIApp) Run(ctx context.Context) error {
	return a.server.Run(ctx)
}
```

Grep по `Start(` в непроверочном коде показывает вызовы только в `worker_app.go`. То есть в `api_app.go:219` создаётся реальный `kafka.Reader` (устанавливает соединение, вступает в consumer group `bff-bulk-worker`), но `Start()` для него не вызывается никогда.

Следствия, по возрастанию тяжести:

1. **`pendingMessages` всегда 0.** `reader.Stats().Lag` заполняется в процессе чтения; без `FetchMessage` лаг остаётся нулевым. Bulk Service, который по замыслу ТЗ дергает `wake` для проверки, всегда получает «BFF читает Kafka, очередь пуста» — даже если в топике накопились тысячи сообщений, а worker лежит.
2. **API вступает в ту же consumer group, что и worker** (`KAFKA_GROUP_ID: bff-bulk-worker`, одинаковый в `docker-compose.prod.yml` для обоих сервисов). Kafka назначает участнику группы партиции. Партиции, отданные API-процессу, **не обрабатываются никем** — API их не читает. При 1 партиции и неудачном распределении bulk-обработка встаёт полностью. Это уже не только наблюдаемость, но и функциональный отказ (пересекается с A-04).
3. **Ресурсы.** Reader держит соединения и heartbeat-горутины впустую, участвует в ребалансировках, замедляя их.

**Предлагаемый багфикс.**

Правильное решение — **не создавать консьюмер в API вообще**. Замысел эндпоинта (мониторинг лага) должен реализовываться через отдельный read-only механизм, не вступающий в consumer group:

```go
// internal/ports/ports.go
type LagReader interface {
    // Lag возвращает лаг consumer group по топику, не вступая в группу.
    Lag(ctx context.Context, topic, groupID string) (int64, error)
}
```

```go
// internal/adapters/kafka/lag_reader.go
type AdminLagReader struct {
    client *kafka.Client
}

func (r *AdminLagReader) Lag(ctx context.Context, topic, groupID string) (int64, error) {
    // kafka.Client.OffsetFetch + ListOffsets: читает committed offset группы
    // и last offset партиций, не присоединяясь к группе.
    ...
}
```

Хендлер:

```go
func (h *APIHandler) BulkWake(c *gin.Context) {
	lag, err := h.lagReader.Lag(c.Request.Context(), kafkaadapter.TopicBulkTasks, h.groupID)
	if err != nil {
		// Явно сообщаем, что состояние неизвестно, а не выдаём 0 за правду
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "unknown",
			"error":  "cannot read consumer group lag",
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"status":          "awake",
		"pendingMessages": lag,
	})
}
```

И убрать создание `bulkConsumer` из `api_app.go` целиком (это же снимает F-01 и часть A-04).

Минимальный вариант, если рефакторинг откладывается: экспонировать лаг метрикой из **worker'а** (где консьюмер реально работает) и в `wake` отдавать `"pendingMessages": null` с честным `"source": "unavailable"`, чтобы не выдавать 0 за достоверное значение.

**Комментарий.** Классический случай «мониторинг, который врёт» — хуже отсутствия мониторинга, потому что создаёт ложную уверенность. Причём здесь дефект двойной: недостоверная метрика **и** молчаливое воровство партиций у worker'а. Второе опаснее и заслуживает отдельного приоритета: это функциональный отказ bulk-обработки, замаскированный под особенность мониторинга.

---

## F-08. `PendingCount()` использует деструктивный `Stats()`

**Суть.** `kafka-go` документирует `Reader.Stats()` как снапшот **с момента последнего вызова** — то есть чтение сбрасывает счётчики. Использование его для отдачи значения в HTTP-эндпоинте означает, что конкурентные вызовы получают разные результаты, а любой другой потребитель `Stats()` теряет данные.

**Место.**
- `internal/adapters/kafka/consumer.go:110-117` — `PendingCount()`
- `$GOMODCACHE/github.com/segmentio/kafka-go@*/reader.go:1089` — документация

**Подробное описание.**

Код:

```go
func (c *Consumer) PendingCount() int {
	stats := c.reader.Stats()
	lag := stats.Lag
	if lag < 0 {
		return 0
	}
	return int(lag)
}
```

Исходник библиотеки (`reader.go:1089`):

```go
// Stats returns a snapshot of the reader stats since the last time the method
// was called.
```

и далее по полям — `r.stats.dials.snapshot()`, `r.stats.fetches.snapshot()` и т.д., где `snapshot()` выполняет atomic swap на ноль.

Практические следствия:

1. **Гонка между конкурентными вызовами `wake`.** Два одновременных запроса: первый получает накопленное значение, второй — почти нулевое. Мониторинг видит «пилу», не отражающую реальность.
2. **Конфликт с любым будущим экспортом метрик.** Если добавить (как предложено в F-05/F-09) периодический сбор `Stats()` для Prometheus, он и `wake` начнут отбирать данные друг у друга.
3. Формально `Lag` — это gauge-подобная величина, но в `kafka-go` она попадает под ту же логику снапшота, поэтому полагаться на её накопление нельзя.

Замечание: сейчас дефект **латентный**, потому что `Start()` не вызывается (F-07) и лаг всегда 0. Он проявится ровно в тот момент, когда F-07 исправят «наивно» — добавив `Start()` в `APIApp.Run`.

**Предлагаемый багфикс.**

Не использовать `Stats()` как источник истины для внешнего API. Если консьюмер всё же остаётся в процессе, вести собственный gauge, обновляемый в цикле чтения:

```go
type Consumer struct {
    ...
    lastLag atomic.Int64
}

// в Start(), после успешного FetchMessage:
c.lastLag.Store(c.reader.Lag())  // Reader.Lag() — не деструктивный

func (c *Consumer) PendingCount() int {
    lag := c.lastLag.Load()
    if lag < 0 {
        return 0
    }
    return int(lag)
}
```

`Reader.Lag()` (в отличие от `Stats().Lag`) возвращает текущее значение без сброса. Дополнительно экспонировать его метрикой:

```go
KafkaConsumerLag = promauto.NewGaugeVec(
    prometheus.GaugeOpts{
        Name: "bff_kafka_consumer_lag",
        Help: "Current consumer group lag by topic.",
    },
    []string{"topic", "group"},
)
```

**Комментарий.** Тонкий дефект, который легко пропустить при чтении кода — нужно знать семантику библиотеки. Важен как «мина» на пути исправления F-07: если кто-то решит просто дописать `Start()` в `APIApp.Run`, он одновременно активирует и воровство партиций, и эту гонку. Поэтому F-07 и F-08 надо рассматривать вместе.

---

## F-09. Нет метрик Billing / AI / History / Kafka / Redis

**Суть.** Инструментирован только BarcodeGen. Остальные пять внешних зависимостей вызываются без счётчиков и без гистограмм латентности. Деградацию Billing, AI или Redis нельзя увидеть в метриках.

**Место.**
- `internal/adapters/billing/http_client.go` — нет вызовов `metrics.*`
- `internal/adapters/ai/http_client.go` — нет
- `internal/adapters/history/http_client.go` — нет
- `internal/adapters/events/kafka_publisher.go` — нет
- `internal/adapters/idempotency/redis_store.go`, `redis_locker.go` — нет

**Подробное описание.**

Полный список call-site'ов метрик в проекте (grep `metrics\.`):

```
internal/transport/http/gin/idempotency_middleware.go:77,97  DuplicateRequestsTotal
internal/transport/http/gin/middleware.go:45                 RequestsTotal
internal/usecase/chain_executor.go:38                        ChainExecutionDurationMs
internal/usecase/generate_usecase.go:300                     PartialSuccessTotal
internal/usecase/generate_usecase.go:429,434,437,446         BarcodeGenCallsTotal
```

Ни один адаптер не инструментирован. Что это означает на практике:

- **Латентности нет ни у одного downstream-вызова.** Даже у BarcodeGen есть только счётчик по статусам, но не гистограмма времени. При этом таймаут BarcodeGen — 30 с, AI — 60 с. Рост p99 с 2 с до 25 с не будет виден, пока не начнутся таймауты.
- **Billing.** Самая критичная зависимость (деньги) — полностью тёмная. Нельзя отличить «Billing медленный» от «Billing отказывает».
- **AI.** Таймаут 60 с, вызывается для генерации подписи/фото. При деградации AI общее время запроса растёт до 255+ с (см. A-02), но метрик нет.
- **Redis.** По B-08 ошибки Redis маскируются в `409`. Без метрик Redis-операций диагностировать это невозможно: в `bff_requests_total{status="error"}` будет рост, а причину не видно.
- **Kafka publisher.** Провал публикации события `barcode.generated` означает, что History/Notifications не узнают о генерации. Не наблюдаемо.

**Предлагаемый багфикс.**

Единая метрика для всех исходящих вызовов, чтобы не плодить по счётчику на сервис:

```go
// internal/metrics/metrics.go

// DownstreamCallsTotal — исходящие вызовы. Labels: service, operation, status
DownstreamCallsTotal = promauto.NewCounterVec(
    prometheus.CounterOpts{
        Name: "bff_downstream_calls_total",
        Help: "Total outbound calls to downstream services.",
    },
    []string{"service", "operation", "status"},
)

// DownstreamDurationSeconds — латентность исходящих вызовов.
DownstreamDurationSeconds = promauto.NewHistogramVec(
    prometheus.HistogramOpts{
        Name:    "bff_downstream_duration_seconds",
        Help:    "Outbound call duration in seconds.",
        Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
    },
    []string{"service", "operation"},
)
```

Инструментировать в общем помощнике каждого клиента (у `barcodegen` и `billing` уже есть приватный `post()` — идеальная точка):

```go
func (c *HTTPClient) post(ctx context.Context, path string, in, out any, operation string) error {
    start := time.Now()
    err := c.doPost(ctx, path, in, out)
    status := "success"
    if err != nil {
        status = "error"
    }
    metrics.DownstreamCallsTotal.WithLabelValues("billing", operation, status).Inc()
    metrics.DownstreamDurationSeconds.WithLabelValues("billing", operation).
        Observe(time.Since(start).Seconds())
    return err
}
```

Для Redis — обёртка вокруг клиента или явные вызовы в `Get`/`Set`/`Reserve`/`Delete`.

Bucket до 60 с обязателен: при таймаутах AI 60 с и BarcodeGen 30 с стандартные бакеты Prometheus (до 10 с) обрежут всё интересное в `+Inf`.

**Комментарий.** Пробел системный: инструментирован ровно один сервис из шести, вероятно потому, что он упомянут в пункте ТЗ про метрики. В результате мониторинг покрывает наименее проблемную зависимость (у BarcodeGen хотя бы есть ретраи), а самую критичную — Billing — не покрывает. Стоит делать одним заходом с F-06: обе задачи трогают те же файлы.

---

## F-10. Нет алертов

**Суть.** В `monitoring/prometheus.yml` отсутствуют `rule_files` и `alerting`. Alertmanager не развёрнут. Ни одно из состояний, найденных в разделах A–E, не приводит к оповещению — только к записи в лог, который никто не читает.

**Место.**
- `monitoring/prometheus.yml` — 10 строк, только `global` и `scrape_configs`
- `docker-compose.observability.yml` — нет сервиса `alertmanager`
- Нет файла `monitoring/alerts.yml`

**Подробное описание.**

`prometheus.yml` целиком:

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: barcode-bff
    metrics_path: /metrics
    static_configs:
      - targets:
          - bff:8080
```

`evaluation_interval` задан, но правил для вычисления нет. Стек наблюдаемости (Prometheus + Loki + Promtail + Grafana) собран целиком в режиме «посмотреть глазами»: есть дашборд с двумя панелями, и всё.

Список инцидентов из аудита, которые сейчас происходят молча:

| Инцидент | Раздел | Как обнаруживается сейчас |
|----------|--------|---------------------------|
| Деньги заблокированы, `Release` не прошёл | A-01, A-04 | Жалоба пользователя |
| Двойное списание | B-04, B-06 | Жалоба пользователя |
| Два бесплатных редактирования | A-02 | Расхождение в отчётности |
| Работа на mock-адаптерах в проде | E-04..E-06 | Никак |
| Пустой `ADMIN_TOKEN`, админка открыта | E-08 | Никак |
| Redis лежит, все запросы отбиваются `409` | B-08 | Жалобы пользователей |
| Рост DLQ | — | Никак |

**Предлагаемый багфикс.**

`monitoring/alerts.yml`:

```yaml
groups:
  - name: bff-critical
    interval: 30s
    rules:
      # Деньги зависли — требует ручной сверки с Billing
      - alert: BFFCompensationFailure
        expr: increase(bff_compensation_failures_total[5m]) > 0
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "Компенсация Billing провалилась — средства заблокированы"
          description: "operation={{ $labels.operation }} reason={{ $labels.reason }}. Нужна ручная сверка."

      # Сервис работает на моках (E-04..E-06)
      - alert: BFFRunningOnMocks
        expr: bff_mock_adapters_active > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "BFF использует mock-адаптеры — генерация может быть бесплатной"

      - alert: BFFHighErrorRate
        expr: |
          sum(rate(bff_requests_total{status="error"}[5m]))
            / sum(rate(bff_requests_total[5m])) > 0.05
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Доля ошибок BFF выше 5%"

      - alert: BFFBillingDown
        expr: |
          sum(rate(bff_downstream_calls_total{service="billing",status="error"}[5m]))
            / sum(rate(bff_downstream_calls_total{service="billing"}[5m])) > 0.5
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Более половины вызовов Billing падают"

  - name: bff-warning
    interval: 30s
    rules:
      - alert: BFFTargetDown
        expr: up{job=~"barcode-bff.*"} == 0
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Prometheus не может скрейпить {{ $labels.instance }}"

      - alert: BFFKafkaLagGrowing
        expr: bff_kafka_consumer_lag > 1000
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Лаг consumer group растёт — worker не успевает"

      - alert: BFFDLQGrowing
        expr: increase(bff_dlq_messages_total[15m]) > 10
        for: 0m
        labels:
          severity: warning
        annotations:
          summary: "Сообщения уходят в DLQ"

      - alert: BFFPartialSuccessSpike
        expr: increase(bff_saga_outcome_total{outcome="partial"}[10m]) > 5
        for: 0m
        labels:
          severity: warning
        annotations:
          summary: "Рост частичных генераций"
```

`monitoring/prometheus.yml`:

```yaml
rule_files:
  - /etc/prometheus/alerts.yml

alerting:
  alertmanagers:
    - static_configs:
        - targets: ["alertmanager:9093"]
```

И сервис в `docker-compose.observability.yml`:

```yaml
  alertmanager:
    image: prom/alertmanager:v0.27.0
    volumes:
      - ./monitoring/alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro
      - alertmanager-data:/alertmanager
    ports:
      - "9093:9093"
    restart: unless-stopped
```

**Комментарий.** Часть правил опирается на метрики, которых пока нет (F-06, F-09), поэтому порядок: сначала метрики, потом алерты. Но проектировать их стоит вместе — набор метрик определяется тем, на что нужно алертить, а не наоборот. Обратите внимание на `BFFRunningOnMocks`: это единственный способ технически поймать fail-open из раздела E, если не делать полный strict-режим конфигурации.

---

## F-11. Grafana-дашборд использует 2 метрики из 5

**Суть.** Единственный дашборд содержит 3 панели и опирается на две метрики. `partial_success`, `duplicate_requests` и гистограмма `chain_execution_duration` не визуализированы нигде.

**Место.**
- `monitoring/grafana/dashboards/barcode-bff-overview.json`
- `monitoring/grafana/provisioning/dashboards/dashboards.yml`

**Подробное описание.**

Извлечённые из дашборда запросы:

```
sum(increase(bff_requests_total[5m]))
sum by (status) (rate(bff_barcodegen_calls_total[5m]))
{compose_service=~...}          # Loki-панель с логами
```

Названия панелей: `Requests / 5m`, `BarcodeGen calls rate`, `BFF / Worker logs`.

Не покрыто:

- `bff_partial_success_total` — частичные генерации, прямое денежное последствие (п.5 ТЗ);
- `bff_duplicate_requests_total` — работа идемпотентности;
- `bff_chain_execution_duration_ms` — латентность Chain Executor, единственная существующая гистограмма.

Кроме того, панель `Requests / 5m` использует `sum(increase(...))` без разбивки по `status` и `endpoint` — то есть по ней нельзя увидеть даже соотношение успехов и ошибок, хотя метка `status` в метрике есть.

Отдельно: панель логов фильтрует по `compose_service`, то есть дашборд жёстко привязан к docker-compose и в Kubernetes не заработает.

**Предлагаемый багфикс.**

Дополнить дашборд панелями (в порядке пользы для дежурного):

1. **Error rate** — `sum(rate(bff_requests_total{status="error"}[5m])) / sum(rate(bff_requests_total[5m]))`, с порогом 5%.
2. **Saga outcomes** — `sum by (outcome) (rate(bff_saga_outcome_total[5m]))` (после F-06).
3. **Compensation failures** — `increase(bff_compensation_failures_total[5m])`, stat-панель с красным при > 0.
4. **Downstream latency p50/p95/p99** — `histogram_quantile(0.99, sum by (le, service) (rate(bff_downstream_duration_seconds_bucket[5m])))` (после F-09).
5. **Chain executor duration** — `histogram_quantile(0.95, rate(bff_chain_execution_duration_ms_bucket[5m]))`.
6. **Idempotency** — `rate(bff_duplicate_requests_total[5m])` и `rate(bff_idempotency_events_total[5m])` по метке `event`.
7. **Kafka lag / DLQ** — после F-05 и F-08.
8. **Requests by endpoint** — `topk(10, sum by (endpoint) (rate(bff_requests_total[5m])))`.

В существующей панели `Requests / 5m` заменить запрос на `sum by (status) (increase(bff_requests_total[5m]))`.

**Комментарий.** Низкий приоритет сам по себе, но полезный индикатор: дашборд отражает не то, что важно, а то, что было проще всего построить. После добавления метрик из F-06/F-09 его надо переделывать целиком, поэтому разумно отложить до тех фиксов.

---

## F-12. У `worker` нет healthcheck в compose

**Суть.** Сервис `bff` в docker-compose имеет healthcheck, а `worker` — нет. Зависший worker (например, в ребалансировке или на мёртвом соединении с Kafka) остаётся в статусе `running`, и `restart: unless-stopped` его не перезапускает.

**Место.**
- `docker-compose.yml:53-73` — сервис `worker`, healthcheck отсутствует
- `docker-compose.yml:47-52` — healthcheck у `bff` для сравнения
- `Dockerfile:36-41` — стадия `worker` без `HEALTHCHECK`

**Подробное описание.**

Определение сервиса `worker`:

```yaml
  worker:
    build:
      context: .
      target: worker
    volumes:
      - ./configs:/app/configs
    environment:
      ...
    depends_on:
      - kafka
    restart: unless-stopped
```

`restart: unless-stopped` реагирует только на **выход процесса**. Worker, который остался жив, но перестал обрабатывать сообщения, не будет перезапущен. Реалистичные сценарии:

- Kafka reader в бесконечной ребалансировке из-за конфликта consumer group с API-процессом (F-07);
- зависший HTTP-вызов без таймаута (у BarcodeGen 30 с есть, но при отсутствии `ENV` возможны дефолты);
- дедлок на мьютексе store'а (A-09: `os.WriteFile` под мьютексом, который читается на каждом исходящем вызове).

Стадия `worker` в `Dockerfile` тоже без `HEALTHCHECK` — в отличие от стадии `api`. Это логично, пока у worker нет HTTP-порта, и снимается вместе с F-05.

**Предлагаемый багфикс.**

После добавления HTTP-сервера метрик (F-05):

```dockerfile
# Dockerfile, стадия worker
FROM alpine:3.19 AS worker

RUN apk --no-cache add ca-certificates tzdata wget
WORKDIR /app

COPY --from=builder /worker ./worker
COPY --from=builder /app/configs ./configs

EXPOSE 8081
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:8081/livez || exit 1

CMD ["./worker"]
```

```yaml
  worker:
    ...
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:8081/livez"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 10s
```

Более содержательная проверка — liveness, отражающий факт прогресса обработки:

```go
// worker: отдавать 503, если последнее успешное чтение из Kafka было слишком давно
var lastProgress atomic.Int64 // unix ts, обновляется в цикле консьюмера

mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
    // порог должен быть больше нормальной паузы между сообщениями
    if time.Since(time.Unix(lastProgress.Load(), 0)) > 10*time.Minute {
        w.WriteHeader(http.StatusServiceUnavailable)
        _, _ = w.Write([]byte(`{"status":"stalled"}`))
        return
    }
    w.WriteHeader(http.StatusOK)
    _, _ = w.Write([]byte(`{"status":"alive"}`))
})
```

Осторожно с порогом: в норме bulk-топик может быть пуст часами, и «нет сообщений» не равно «зависли». Правильнее обновлять `lastProgress` не только при обработке сообщения, но и при успешном опросе (в том числе пустом).

**Комментарий.** Связан с F-05: оба решаются одним изменением — добавлением служебного HTTP-порта в worker. Приоритет средний, потому что до исправления F-01 worker вообще не собирается.

---

## F-13. Логи без структуры и уровней

**Суть.** Везде используется стандартный `log.Printf` с текстовыми сообщениями. Нет уровней (`debug`/`info`/`warn`/`error`), нет структурированных полей, нет JSON. Loki собирает эти строки, но фильтровать по `userID` или `sagaID` можно только регулярками.

**Место.** Все вызовы `log.Printf` / `log.Fatalf` в `internal/` и `cmd/`, в частности:
- `internal/app/api_app.go:43,60,63,217,222` — предупреждения о моках и конфигах
- `internal/adapters/kafka/consumer.go:47,51,58` — жизненный цикл консьюмера
- `cmd/api/main.go`, `cmd/worker/main.go` — `log.Fatalf`

**Подробное описание.**

Характерные примеры:

```go
log.Printf("warn: cannot load topup-bonus config from %s: %v (using defaults)", path, err)
log.Printf("api bulk.tasks: using real Kafka consumer → brokers=%s group=%s", brokers, cfg.Kafka.GroupID)
log.Printf("bulk.tasks consumer: close error: %v", err)
```

Проблемы:

1. **Уровень зашит в текст.** «Настоящесть» warning выражена префиксом `"warn: "` в одних местах и никак — в других. В Loki/Grafana нельзя сделать `| level="error"`; только `|~ "warn:"`, что ломается при любой опечатке.
2. **Нет структурированных полей.** Ни `userID`, ни `sagaID`, ни `traceID` (которого нет вовсе — F-04) не выделены в метки. Найти все записи по одной саге — полнотекстовый поиск.
3. **Нет управления verbosity.** Нельзя включить debug в проде без пересборки.
4. **`log.Fatalf` в `main`** пишет в stderr и вызывает `os.Exit(1)`, минуя `defer`. В `cmd/worker/main.go` есть `defer worker.Close()` и `defer stop()` — при `log.Fatalf` они **не выполнятся**, то есть Kafka Writer не закроется корректно (пересекается с A-06).
5. **PII в логах** — отдельно разобрано в E-13; структурированное логирование упрощает редактирование чувствительных полей.

**Предлагаемый багфикс.**

Перейти на `slog` из стандартной библиотеки (Go 1.25, зависимостей не нужно):

```go
// internal/logging/logging.go
package logging

import (
    "context"
    "log/slog"
    "os"
    "strings"
)

func Setup(levelStr string, jsonOutput bool) *slog.Logger {
    var level slog.Level
    switch strings.ToLower(levelStr) {
    case "debug":
        level = slog.LevelDebug
    case "warn":
        level = slog.LevelWarn
    case "error":
        level = slog.LevelError
    default:
        level = slog.LevelInfo
    }

    opts := &slog.HandlerOptions{Level: level}
    var h slog.Handler
    if jsonOutput {
        h = slog.NewJSONHandler(os.Stdout, opts)
    } else {
        h = slog.NewTextHandler(os.Stdout, opts)
    }
    l := slog.New(h)
    slog.SetDefault(l)
    return l
}

// FromContext возвращает логгер с traceID из контекста (см. F-04).
func FromContext(ctx context.Context) *slog.Logger {
    if tid := TraceIDFrom(ctx); tid != "" {
        return slog.Default().With("traceId", tid)
    }
    return slog.Default()
}
```

Пример миграции критичного места:

```go
// было
log.Printf("CRITICAL: release failed: saga=%s units=%d err=%v", sagaID, units, err)

// стало
logging.FromContext(ctx).Error("billing release failed, funds remain blocked",
    "sagaId", sagaID,
    "userId", userID,
    "units", units,
    "error", err,
)
```

Заменить `log.Fatalf` в `main` на явный выход после отработки defer:

```go
if err := worker.Run(ctx); err != nil {
    slog.Error("worker stopped with error", "error", err)
    worker.Close()          // явно, т.к. os.Exit не вызовет defer
    os.Exit(1)
}
```

Добавить в конфиг `LOG_LEVEL` и `LOG_FORMAT` (`json` в проде, `text` в dev) — тогда promtail сможет парсить JSON и выделять поля метками автоматически.

**Комментарий.** Само по себе — средний приоритет, но это множитель для F-04: структурированные логи без `traceID` малополезны, а `traceID` без структурированных логов приходится грепать. Делать вместе. Дополнительный бонус — исправление `log.Fatalf`, которое закрывает часть проблемы некорректного завершения из A-06.

---

## F-14. `MetricsMiddleware` не пишет латентность HTTP

**Суть.** Middleware считает только количество запросов. Гистограммы длительности HTTP-запросов нет ни одной. При том что запрос на генерацию может законно длиться до ~255 с, нельзя построить ни p99, ни SLO.

**Место.**
- `internal/transport/http/gin/middleware.go:40-46` — `MetricsMiddleware`
- `internal/metrics/metrics.go` — нет `HTTPDurationSeconds`

**Подробное описание.**

Middleware:

```go
func MetricsMiddleware() gin.HandlerFunc {
    return func(c *gin.Context) {
        // ...
        c.Next()
        status := "success"
        if c.Writer.Status() >= 400 {
            status = "error"
        }
        metrics.RequestsTotal.WithLabelValues(c.FullPath(), status).Inc()
    }
}
```

Время выполнения не измеряется, хотя middleware — идеальное место: он уже оборачивает весь запрос.

Почему это важно именно здесь. Из раздела A: худший сценарий одного запроса на генерацию — 3 попытки × 30 с таймаута BarcodeGen + 4 с паузы + AI до 60 с, суммарно ~255 с плюс N × 94 с на элемент. Без гистограммы:

- нельзя понять, приближается ли типичный запрос к таймауту;
- нельзя обосновать значения `editLockTTL` и `shortTTL` (A-02, A-03 — там TTL не сходятся с реальным временем, и именно измерений не хватало, чтобы это заметить);
- нельзя задать SLO и алертить на его нарушение.

Также единственная существующая гистограмма (`ChainExecutionDurationMs`) измеряет в **миллисекундах**, тогда как соглашение Prometheus — секунды (`_seconds`). Это создаёт несогласованность единиц в дашбордах.

**Предлагаемый багфикс.**

```go
// internal/metrics/metrics.go
HTTPDurationSeconds = promauto.NewHistogramVec(
    prometheus.HistogramOpts{
        Name: "bff_http_request_duration_seconds",
        Help: "HTTP request duration in seconds.",
        // Верхние бакеты обязательны: генерация законно длится минуты (см. A-01)
        Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300},
    },
    []string{"endpoint", "method", "status_class"},
)
```

```go
// internal/transport/http/gin/middleware.go
func MetricsMiddleware() gin.HandlerFunc {
    return func(c *gin.Context) {
        start := time.Now()
        c.Next()

        endpoint := c.FullPath()
        if endpoint == "" {
            endpoint = "unmatched" // см. F-03: 404 иначе схлопываются в пустую метку
        }
        code := c.Writer.Status()

        status := "success"
        if code >= 400 {
            status = "error"
        }
        metrics.RequestsTotal.WithLabelValues(endpoint, status).Inc()

        metrics.HTTPDurationSeconds.WithLabelValues(
            endpoint, c.Request.Method, statusClass(code),
        ).Observe(time.Since(start).Seconds())
    }
}

func statusClass(code int) string {
    switch {
    case code < 300:
        return "2xx"
    case code < 400:
        return "3xx"
    case code < 500:
        return "4xx"
    default:
        return "5xx"
    }
}
```

Заодно переименовать `ChainExecutionDurationMs` → `bff_chain_execution_duration_seconds` с бакетами в секундах, для единообразия.

**Комментарий.** Дефект, который напрямую помешал заметить другие: в разделе A пришлось вычислять временные бюджеты вручную из констант конфига именно потому, что измерений нет. Гистограмма латентности сделала бы расхождение `editLockTTL = 30 с` против реального цикла 50 с (A-02) очевидным на графике.

---

## F-15. Метрика ошибок схлопывает 4xx и 5xx в `error`

**Суть.** `MetricsMiddleware` делит статусы на `success` и `error` по порогу 400. Клиентские ошибки валидации и серверные отказы попадают в одну метку, поэтому по метрикам невозможно отличить «клиент прислал мусор» от «сервис сломан».

**Место.**
- `internal/transport/http/gin/middleware.go:42-45`
- `internal/metrics/metrics.go:11-19` — метки `endpoint`, `status`

**Подробное описание.**

Проба показала фактическое поведение:

```
PROBE F-03: label endpoint=/api/v1/barcode/:id status=success -> 3
PROBE F-03: label endpoint=/api/v1/boom status=error -> 1
PROBE F-03: label endpoint="" status=error -> 1 (404 агрегируются в пустую метку)
```

То есть в `status="error"` попадают вместе:

- `400 VALIDATION_ERROR` — клиент прислал некорректные поля;
- `402 INSUFFICIENT_FUNDS` — у пользователя нет денег (нормальное бизнес-состояние);
- `409 REQUEST_IN_FLIGHT` — дубликат... **или падение Redis** (B-08);
- `500 BARCODEGEN_ERROR` — реальный отказ.

Практический эффект: алерт вида «error rate > 5%» (предложенный в F-10) будет ложно срабатывать при всплеске `402 INSUFFICIENT_FUNDS` — а это не инцидент, а поведение пользователей. И наоборот: рост 500-х может утонуть в фоне 400-х.

Второе наблюдение из пробы: при 404 `c.FullPath()` возвращает пустую строку, и все несуществующие пути схлопываются в метку `endpoint=""`. Это, кстати, **защищает** от cardinality-атаки (см. F-21), но делает метку неинформативной.

**Предлагаемый багфикс.**

Добавить класс статуса и код ошибки домена, сохранив обратную совместимость по `status`:

```go
metrics.RequestsTotal.WithLabelValues(endpoint, status).Inc() // оставить как есть

// Новая, с разбором по классам
metrics.HTTPResponsesTotal.WithLabelValues(
    endpoint,
    c.Request.Method,
    statusClass(code),        // "2xx"/"4xx"/"5xx"
    c.GetString("errorCode"), // "VALIDATION_ERROR", "INSUFFICIENT_FUNDS", "" для успеха
).Inc()
```

`errorCode` заполняется в `RespondError`, где `AppError` уже доступен:

```go
// internal/transport/http/gin/error_handler.go
func RespondError(c *gin.Context, err error) {
    var appErr *domain.AppError
    if errors.As(err, &appErr) {
        c.Set("errorCode", appErr.Code) // для метрики
        c.AbortWithStatusJSON(appErr.HTTPStatus, appErr)
        return
    }
    // ...
}
```

Тогда алерт из F-10 можно уточнить, исключив бизнес-состояния:

```yaml
expr: |
  sum(rate(bff_http_responses_total{status_class="5xx"}[5m]))
    / sum(rate(bff_http_responses_total[5m])) > 0.05
```

Осторожно с cardinality: `errorCode` — закрытое множество из `apperrors.go` (около десятка значений), это безопасно. Нельзя добавлять в метки текст ошибки или ID.

**Комментарий.** Прямо влияет на качество алертов из F-10: без разделения 4xx/5xx любой порог по error rate будет либо шумным, либо слепым. Правится в том же месте, что F-14, — разумно делать одним изменением middleware.

---

## F-16. Артефакты покрытия закоммичены в репозиторий

**Суть.** В корне лежат `cover_all` (63 КБ) и `coverage_target` (15 КБ) — выводы профилировщика покрытия. `.gitignore` игнорирует `coverage.out` и `*.out`, но не эти имена, поэтому они попали под версионный контроль.

**Место.**
- `/cover_all` — 63602 байт
- `/coverage_target` — 15567 байт
- `/.gitignore` — правила `coverage.out`, `*.out`, `.coverprofile`, `coverage.html`

**Подробное описание.**

Проверка формата опровергла первоначальное предположение о склейке нескольких профилей:

```
=== cover_all mode lines ===
1
=== go tool cover validity ===
github.com/ikermy/BFF/cmd/api/main.go:13:   main   0.0%
github.com/ikermy/BFF/cmd/worker/main.go:13: main  0.0%
...
```

То есть `cover_all` **валиден** (одна строка `mode:`), `go tool cover -func` его читает. Это уточнение к более раннему выводу: файл не битый.

Тем не менее проблемы остаются:

1. **Бинарные по смыслу артефакты в git.** Меняются при каждом прогоне тестов, дают шумные диффы, растят историю репозитория.
2. **Немедленно устаревают.** Данные покрытия относятся к какому-то прошлому состоянию кода. Сейчас, когда HEAD не собирается (F-01), профиль заведомо не соответствует коду и вводит в заблуждение — по нему `main` показан как 0.0%, что верно, но `internal/app` в нём вообще нет данных о текущем состоянии.
3. **Нестандартные имена** обходят `.gitignore` — то есть проблема воспроизведётся при следующем прогоне с теми же именами.

**Предлагаемый багфикс.**

Убрать из индекса и расширить `.gitignore`:

```
# Coverage artifacts
coverage.out
coverage.html
.coverprofile
cover_all
coverage_target
cover*.out
*.cover
```

```bash
git rm --cached cover_all coverage_target
```

И добавить воспроизводимую цель в `Makefile` вместо ручных файлов (см. F-19):

```makefile
coverage:
	go test ./... -coverprofile=coverage.out -covermode=atomic
	go tool cover -func=coverage.out | tail -1
	go tool cover -html=coverage.out -o coverage.html
```

**Комментарий.** Косметика, но с содержательным следствием: покрытие, зафиксированное файлом в репозитории, вместо порога в CI — это метрика, которую нельзя проверить. Правильнее считать покрытие в CI (F-20) и падать при снижении ниже порога, чем хранить снапшот.

---

## F-17. Дубль `internal/app/configs/` из-за относительных путей

**Суть.** Admin-конфиги (`timeouts.yaml`, `topup-bonus.yaml`) читаются и пишутся по относительным путям. Из-за этого пришлось положить копию под `internal/app/configs/`, чтобы тесты находили файлы из своей рабочей директории. Теперь два источника истины, которые расходятся.

**Место.**
- `configs/admin/timeouts.yaml`, `configs/admin/topup-bonus.yaml` — оригиналы
- `internal/app/configs/admin/timeouts.yaml`, `internal/app/configs/admin/topup-bonus.yaml` — дубли
- `internal/config/paths.go` — определения путей
- `internal/app/api_app.go:59-64` — загрузка с логированием warning при неудаче
- `Dockerfile:19,38` — `COPY --from=builder /app/configs ./configs`
- `docker-compose.yml:10-11` — `volumes: ./configs:/app/configs`

**Подробное описание.**

Загрузка устроена как «попробовать, при неудаче — дефолты»:

```go
if err := topUpBonusStore.LoadFromFile(config.TopupBonusConfigPath); err != nil {
    log.Printf("warn: cannot load topup-bonus config from %s: %v (using defaults)", config.TopupBonusConfigPath, err)
}
if err := timeoutStore.LoadFromFile(config.TimeoutsConfigPath); err != nil {
    log.Printf("warn: cannot load timeouts config from %s: %v (using defaults)", config.TimeoutsConfigPath, err)
}
```

Так как путь относительный, результат зависит от рабочей директории процесса:

| Контекст | CWD | Что читается |
|----------|-----|--------------|
| `./bff` в контейнере | `/app` | `/app/configs/admin/*` (смонтированный том) |
| `go test ./internal/app/` | `internal/app/` | `internal/app/configs/admin/*` (дубль) |
| `go run ./cmd/api` из корня | корень репозитория | `configs/admin/*` |

Отсюда три следствия:

1. **Дубль расходится с оригиналом.** Никакой синхронизации нет; правка одного файла не отражается в другом. Тесты проверяют одни значения, прод работает на других.
2. **Тихая деградация.** При неверном CWD конфиг не найден, но сервис поднимается на дефолтах, напечатав warning. Учитывая F-13 (нет уровней) и F-10 (нет алертов), этого никто не заметит. Пересекается с fail-open из раздела E.
3. **Запись в контейнер.** Admin API пишет YAML по тому же относительному пути (A-09). В `docker-compose.yml` том `./configs:/app/configs` смонтирован, так что запись попадёт на хост — но при деплое без тома изменения уйдут в writable-слой контейнера и потеряются при рестарте, а другие реплики их не увидят.

**Предлагаемый багфикс.**

Задавать корень конфигов через ENV с явной проверкой при старте:

```go
// internal/config/paths.go
func ConfigRoot() string {
    if root := os.Getenv("CONFIG_ROOT"); root != "" {
        return root
    }
    return "configs" // dev-дефолт
}

func TimeoutsConfigPath() string {
    return filepath.Join(ConfigRoot(), "admin", "timeouts.yaml")
}
```

В строгом режиме (`APP_ENV=production`) отсутствие конфига должно быть фатальным, а не warning:

```go
if err := timeoutStore.LoadFromFile(config.TimeoutsConfigPath()); err != nil {
    if cfg.AppEnv == "production" {
        log.Fatalf("cannot load timeouts config from %s: %v", config.TimeoutsConfigPath(), err)
    }
    slog.Warn("cannot load timeouts config, using defaults",
        "path", config.TimeoutsConfigPath(), "error", err)
}
```

В тестах — `t.Setenv("CONFIG_ROOT", "testdata/configs")` с фикстурами в `testdata/`, после чего дубль `internal/app/configs/` удаляется.

Стратегически: admin-конфиги, изменяемые через API в рантайме, не должны храниться в файлах контейнера вообще. Их место — Redis или БД, чтобы изменения были атомарными и видимыми всем репликам (это же снимает A-09).

**Комментарий.** Дубль каталога — симптом, а не болезнь. Настоящая проблема — относительные пути плюс fail-open загрузка: сервис умеет запускаться «не с тем» конфигом и не сообщать об этом. Отсюда же растут A-09 (запись под мьютексом) и часть E (недоверенное состояние конфигурации).

---

## F-18. README ссылается на несуществующий `deploy/`

**Суть.** README описывает каталог `deploy/`, которого в репозитории нет. Документация расходится с фактической структурой.

**Место.**
- `README.md` — упоминание `deploy/`
- Корень репозитория — каталога нет (есть `docker-compose*.yml`, `Dockerfile`, `monitoring/`)

**Подробное описание.**

Фактическое содержимое корня: `cmd/`, `configs/`, `docs/`, `internal/`, `monitoring/`, `Dockerfile`, `Makefile`, 5 файлов `docker-compose*.yml`, `cover_all`, `coverage_target`.

Каталога `deploy/` нет. Кроме того README, судя по объёму (21 КБ), описывает архитектуру подробно — тем важнее, чтобы описание совпадало с кодом. Рядом стоит отметить и другие расхождения, найденные в предыдущих разделах:

- README обещает `/admin` с `role=admin` в JWT, фактически это статический shared secret (E-07);
- README описывает `deploy/`, фактически развёртывание через `docker-compose*.yml` + `Makefile`.

**Предлагаемый багфикс.**

Заменить упоминание на фактическую структуру:

```markdown
## Развёртывание

Стек описан в docker-compose файлах и управляется через Makefile:

- `docker-compose.yml` — базовое определение сервисов (bff, worker, kafka, zookeeper, redis)
- `docker-compose.dev.yml` — dev-оверлей (mock downstream)
- `docker-compose.test.yml` — тестовый оверлей (ослабленная авторизация)
- `docker-compose.prod.yml` — prod-подобный оверлей (реальные URL из окружения)
- `docker-compose.observability.yml` — Prometheus, Loki, Promtail, Grafana

Команды: `make dev-up`, `make test-up`, `make prod-up`, `make obs-up`, `make up-all`, `make down`.
```

Полезно добавить в CI шаг проверки ссылок в документации, чтобы такие расхождения не накапливались.

**Комментарий.** Тривиально, но стоит исправить вместе с E-07: README — единственный источник, по которому новый разработчик судит о модели безопасности, и сейчас он описывает более строгую схему, чем реализована. Устаревшая документация опаснее отсутствующей.

---

## F-19. `Makefile` не имеет `build` / `lint` / `vet` / `race`

**Суть.** Из инженерных целей в Makefile есть только `test` (`go test ./... -count=1`). Нет `build`, `vet`, `lint`, `race`, `coverage`, `tidy`. Отсутствие цели `build` — прямая причина, по которой F-01 не был замечен локально.

**Место.** `Makefile` — 12 целей, из которых 11 про docker-compose и одна про тесты.

**Подробное описание.**

Полный список целей: `help`, `dev-up`, `test-up`, `prod-up`, `obs-up`, `up-all`, `down`, `logs`, `ps`, `config-dev`, `config-test`, `config-prod`, `config-obs`, `test`.

То есть Makefile — обёртка вокруг docker-compose плюс `go test`. Следствия:

1. **Нет `make build`.** Разработчик, запустивший `make test`, увидит `FAIL ... [build failed]` для трёх пакетов, но остальные 13 пройдут, и вывод легко принять за «в целом зелено». Отдельная цель `build`, падающая явно, сделала бы F-01 очевидным.
2. **Нет `make vet`.** `go vet` находит реальные проблемы (в том числе F-01).
3. **Нет `make race`.** Гонки из раздела A (A-07, A-08 — алиасинг внутренних структур) детектируются `-race`. В окружении аудита `-race` недоступен (нет cgo-компилятора), поэтому приходилось писать детерминированные пробы; в нормальном окружении это одна команда.
4. **Нет линтера.** Ни `golangci-lint`, ни конфигурации к нему.
5. **`test` без `-race` и без покрытия.**

**Предлагаемый багфикс.**

```makefile
.PHONY: build vet lint test race coverage tidy check

GO ?= go

build:
	$(GO) build ./...

vet:
	$(GO) vet ./...

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint не установлен: https://golangci-lint.run/welcome/install/"; exit 1; }
	golangci-lint run ./...

test:
	$(GO) test ./... -count=1

race:
	$(GO) test ./... -race -count=1

coverage:
	$(GO) test ./... -coverprofile=coverage.out -covermode=atomic
	$(GO) tool cover -func=coverage.out | tail -1

tidy:
	$(GO) mod tidy
	git diff --exit-code go.mod go.sum

# Полная локальная проверка перед коммитом
check: build vet test race
```

И `.golangci.yml` с линтерами, релевантными найденным дефектам:

```yaml
linters:
  enable:
    - errcheck      # непроверенные ошибки (Release/Capture!)
    - govet
    - staticcheck
    - ineffassign   # мёртвые присваивания (deferShutdown в api_app.go)
    - unused
    - bodyclose     # незакрытые HTTP body в адаптерах
    - contextcheck  # context.Background() вместо переданного ctx (A-01!)
    - noctx         # HTTP-запросы без контекста
    - rowserrcheck
```

`contextcheck` заслуживает отдельного внимания: он находит именно тот класс ошибок, что A-01/A-04/B-06 — использование неправильного контекста для компенсирующих операций.

**Комментарий.** Причинно-следственно связан с F-01 и F-20: сборка сломана именно потому, что её никто не проверяет — ни локально (нет `make build`), ни в CI (CI нет). Добавление `make check` — самая дешёвая мера против повторения. Линтер с `contextcheck` и `errcheck` автоматически выявил бы несколько дефектов из разделов A и B.

---

## F-20. Нет CI-конфигурации

**Суть.** В репозитории нет ни `.github/workflows/`, ни `.gitlab-ci.yml`, ни аналогов. Ничто не проверяет сборку, тесты, линт и уязвимости перед мержем. Именно поэтому HEAD с несобирающимся кодом попал в ветку.

**Место.** Отсутствие `.github/`, `.gitlab-ci.yml`, `Jenkinsfile`, `.circleci/` в корне.

**Подробное описание.**

Прямое доказательство последствий — F-01: коммит `e3b3be7 Kafka DLQ` изменил сигнатуру функции, обновил один из двух вызовов и был закоммичен в несобирающемся состоянии. Любой CI с шагом `go build ./...` заблокировал бы это.

Дополнительно без CI отсутствует:

- проверка `gofmt` (единый стиль);
- `go vet` и линтеры;
- прогон с `-race` (критично для раздела A);
- контроль покрытия (вместо него — закоммиченные артефакты, F-16);
- `go mod tidy` проверка (`go.mod` в окружении аудита оказался «грязным»: `go test` требовал `go mod tidy` при добавлении импорта `prometheus/client_model`);
- сканирование уязвимостей (`govulncheck`);
- сборка Docker-образов (проверка, что `Dockerfile` рабочий).

**Предлагаемый багфикс.**

`.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
    branches: [master, main]
  pull_request:

jobs:
  build-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-go@v5
        with:
          go-version: '1.25'
          cache: true

      - name: Verify formatting
        run: |
          if [ -n "$(gofmt -l .)" ]; then
            echo "Файлы не отформатированы:"; gofmt -l .; exit 1
          fi

      - name: Verify go.mod is tidy
        run: |
          go mod tidy
          git diff --exit-code go.mod go.sum

      # Обязательный шаг: именно его отсутствие пропустило F-01
      - name: Build
        run: go build ./...

      - name: Vet
        run: go vet ./...

      - name: Test with race detector
        run: go test ./... -race -count=1 -coverprofile=coverage.out -covermode=atomic

      - name: Coverage summary
        run: go tool cover -func=coverage.out | tail -1

  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.25'
      - uses: golangci/golangci-lint-action@v6
        with:
          version: latest

  vulncheck:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.25'
      - run: go install golang.org/x/vuln/cmd/govulncheck@latest
      - run: govulncheck ./...

  docker:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Build API image
        run: docker build --target api -t bff-api:ci .
      - name: Build worker image
        run: docker build --target worker -t bff-worker:ci .
```

Ключевое: шаг `go test -race` покроет часть раздела A автоматически, а `govulncheck` — зависимости.

**Комментарий.** Единственный пункт раздела F, который предотвращает будущие дефекты, а не описывает существующие. По соотношению «стоимость/эффект» — самый выгодный: один YAML-файл делает невозможным повторение F-01 и автоматически проверяет гонки из раздела A на каждом PR. Ставить сразу после фикса F-01.

---

## F-21. Cardinality метрик — безопасна (опровергнутая гипотеза)

**Суть.** Проверялось предположение, что метка `endpoint` в `bff_requests_total` формируется из фактического URL и позволяет раздуть cardinality (в том числе злонамеренно). **Гипотеза не подтвердилась.**

**Место.**
- `internal/transport/http/gin/middleware.go:45` — `c.FullPath()`
- `internal/metrics/metrics.go:11-19` — метки `endpoint`, `status`

**Подробное описание.**

Опасение было обоснованным: метрика с меткой из пользовательского ввода — классический вектор (каждое уникальное значение создаёт отдельную временную серию, память Prometheus растёт линейно). При наличии `GET /api/v1/barcode/:id` подстановка миллиона разных ID могла бы создать миллион серий.

Проба показала, что этого не происходит:

```
PROBE F-03: label endpoint=/api/v1/barcode/:id status=success -> 3 (3 разных id схлопнулись: cardinality OK)
PROBE F-03: label endpoint=/api/v1/boom status=error -> 1
PROBE F-03: label endpoint="" status=error -> 1 (404 агрегируются в пустую метку)
```

Причина: `c.FullPath()` в Gin возвращает **шаблон маршрута**, а не фактический путь. Три запроса к `/api/v1/barcode/aaa`, `/bbb`, `/ccc` дали одну серию `endpoint="/api/v1/barcode/:id"` со значением 3.

Для несуществующих путей `FullPath()` возвращает пустую строку, поэтому произвольные URL схлопываются в `endpoint=""` — тоже одна серия. То есть неограниченного роста нет ни на сматченных, ни на несматченных маршрутах.

Итоговая cardinality: (число маршрутов + 1) × 2 значения `status` ≈ 40-50 серий. Безопасно.

Единственное замечание — качество, а не безопасность: метка `endpoint=""` для всех 404 неинформативна. Стоит заменить пустое значение на `"unmatched"` (включено в фикс F-14).

**Комментарий.** Оставлено в отчёте намеренно, чтобы гипотезу не проверяли заново: `c.FullPath()` — правильный выбор, и при добавлении новых метрик (F-06, F-09, F-14) это свойство надо сохранить. Важное следствие для будущих фиксов: в метки нельзя добавлять `userID`, `sagaID`, `barcodeID`, `traceID` или текст ошибки — только закрытые множества (`operation`, `status`, `outcome`, `errorCode` из `apperrors.go`).

---

## Сводка по разделу F

### Статистика

| Критичность | Количество |
|-------------|-----------|
| Блокер | 1 (F-01) |
| Критичный | 4 (F-02, F-04, F-05, F-06) |
| Высокий | 4 (F-03, F-07, F-08, F-09, F-10) |
| Средний | 6 (F-11, F-12, F-13, F-14, F-15, F-20) |
| Низкий | 4 (F-16, F-17, F-18, F-19) |
| Опровергнуто | 1 (F-21) |

### Порядок исправления с зависимостями

```
1. F-01  Починить сборку                    ← блокирует ВСЁ остальное
   └─ добавить smoke-тест на internal/app

2. F-20  Настроить CI (build + vet + race)  ← предотвращает регресс F-01
   └─ F-19 Makefile: build/vet/lint/race    (локальный аналог CI)

3. F-03  Развести /livez и /readyz          ← ДО F-02, иначе каскад рестартов
   └─ F-02 Наполнить /readyz проверками

4. F-04  Трассировка (traceID)              ← множитель для всей диагностики
   └─ F-13 Структурированные логи (slog)    (делать вместе)

5. F-06  Метрики саги и компенсации         ← вместе с фиксом A-01
   └─ F-09 Метрики downstream
   └─ F-14 Латентность HTTP                 (вместе с F-15)
   └─ F-15 Разделение 4xx/5xx

6. F-05  Метрики worker + скрейп            ← вместе с F-12 healthcheck
   └─ F-12 Healthcheck worker

7. F-07  Убрать consumer из API             ← закрывает воровство партиций
   └─ F-08 Не использовать Stats()          (мина на пути наивного фикса F-07)

8. F-10  Алерты                             ← ПОСЛЕ F-06 и F-09
   └─ F-11 Дашборд                          (после появления метрик)

9. F-17  CONFIG_ROOT + strict-режим
   F-16  Убрать артефакты покрытия
   F-18  Актуализировать README
```

### Ключевые взаимосвязи с другими разделами

| Дефект F | Усиливает / связан с |
|----------|----------------------|
| F-02 (health не проверяет) | E-04..E-06 (fail-open): деградация есть, обнаружить нечем |
| F-04 (нет trace) | A-01, B-04: инциденты с деньгами неразбираемы |
| F-06 (нет метрик саги) | A-01, A-04: зависшие деньги невидимы |
| F-07 (consumer в API) | A-04: воровство партиций у worker |
| F-08 (Stats деструктивен) | Мина при наивном фиксе F-07 |
| F-10 (нет алертов) | Все инциденты A–E обнаруживаются жалобами |
| F-13 (log.Fatalf) | A-06: defer не выполняется, Kafka Writer не закрыт |
| F-14 (нет латентности) | A-02, A-03: TTL заданы без измерений |
| F-17 (относительные пути) | A-09: запись конфигов под мьютексом |
| F-19/F-20 (нет CI) | Первопричина F-01 |

### Что в разделе F сделано хорошо

Стоит зафиксировать и не потерять при рефакторинге:

- **Стек наблюдаемости развёрнут целиком**: Prometheus + Loki + Promtail + Grafana с провижинингом datasources и дашбордов, отдельным compose-оверлеем и удобными make-целями. Инфраструктура готова — не хватает наполнения.
- **Promtail собирает логи по docker-сокету** с релейблингом на `container`/`service`/`compose_service` — логи worker'а видны, несмотря на F-05.
- **`c.FullPath()` вместо фактического URL** в метках метрик — правильное решение, защищающее от cardinality-взрыва (F-21).
- **Многостадийный `Dockerfile`** с раздельными таргетами `api`/`worker`, `CGO_ENABLED=0`, `-ldflags="-s -w"`, кэширующим слоем зависимостей и `ca-certificates`/`tzdata`.
- **Оверлеи compose по средам** (dev/test/prod/observability) с параметризацией через `${VAR:-default}`.
- **Grafana не имеет анонимного доступа по умолчанию** (`GF_AUTH_ANONYMOUS_ENABLED` по умолчанию `false`).

---

## Приложение: сводка проб раздела F

Все пробы — детерминированные тесты, удалены после подтверждения.

### Проба F-02: `/health` всегда 200

```
PROBE F-02: status=200 body={"status":"ok"}
PROBE F-02: no Billing/BarcodeGen/Redis/Kafka check in handler body -> always 200
```

### Проба F-03/F-21: cardinality и метки

```
PROBE F-03: label endpoint=/api/v1/barcode/:id status=success -> 3 (3 разных id схлопнулись: cardinality OK)
PROBE F-03: label endpoint=/api/v1/boom status=error -> 1
PROBE F-03: 404 на неизвестный путь -> FullPath()="" (пустая метка)
PROBE F-03: label endpoint="" status=error -> 1 (404 агрегируются в пустую метку)
```

### Проба F-04: traceID не пробрасывается

```
PROBE F-04: sent X-Request-Id=client-supplied-trace-abc123
PROBE F-04: response headers = map[Content-Type:[application/json; charset=utf-8]]
PROBE F-04: X-Request-Id in response = "" (пусто = не пробрасывается)
PROBE F-04: no traceId in response body -> клиент не может сослаться на запрос в поддержке
```

### Проверка F-01: сборка

```
$ go build ./...
internal/app/api_app.go:219:81: not enough arguments in call to kafkaadapter.NewConsumer
	have (string, string, func(ctx context.Context, msg domain.BulkJobMessage) error)
	want (string, string, kafka.MessageHandler, kafka.Publisher)

$ go test ./... -count=1
FAIL github.com/ikermy/BFF/cmd/api      [build failed]
FAIL github.com/ikermy/BFF/cmd/worker   [build failed]
FAIL github.com/ikermy/BFF/internal/app [build failed]
(остальные 13 пакетов — ok)
```

### Проверка F-08: семантика `Stats()`

Из `$GOMODCACHE/github.com/segmentio/kafka-go@*/reader.go:1089`:

```
// Stats returns a snapshot of the reader stats since the last time the method
// was called.
```

Поля заполняются через `snapshot()` (atomic swap на ноль) — чтение деструктивно.

### Проверка F-16: валидность профиля покрытия

```
$ grep -c "^mode:" cover_all
1
$ go tool cover -func=cover_all | head -3
github.com/ikermy/BFF/cmd/api/main.go:13:    main    0.0%
github.com/ikermy/BFF/cmd/worker/main.go:13: main    0.0%
...
```

Файл валиден (уточнение к более раннему предположению о склейке нескольких профилей).
