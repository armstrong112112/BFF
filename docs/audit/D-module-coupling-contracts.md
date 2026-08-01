# Раздел D. Связанность интерфейсов и контракты между модулями

Аудит гексагональных границ проекта `github.com/ikermy/BFF`: соответствие портов и адаптеров, расхождения контрактов mock и real, протечки слоёв, обходы архитектуры, дрейф сигнатур.

**Дата:** 2026-08-02
**Ветка:** `codebase-analysis` (base `master`)
**Метод:** статическая трассировка всех 15 портов из `internal/ports/ports.go` к их реализациям и точкам вызова; сверка сигнатур конструкторов; поиск протечек транспортных типов в домен; `go build` для подтверждения дрейфа сигнатур.

---

## Сводка

| ID | Дефект | Критичность | Подтверждён |
|---|---|---|---|
| D-01 | Дрейф сигнатуры `NewConsumer` — HEAD не компилируется | Блокирующая | Да, `go build` |
| D-02 | Реальные адаптеры никогда не возвращают `*domain.AppError` — коды ошибок теряются на границе | Критическая | Да, трассировка |
| D-03 | Mock и real возвращают несовместимые формы ошибок — retry работает только в прод | Критическая | Да, трассировка |
| D-04 | Обход слоёв: `pdf417`/`code128` дергают адаптер напрямую, минуя usecase и биллинг | Критическая | Да, трассировка |
| D-05 | Нет ни одной compile-time проверки соответствия адаптера порту | Высокая | Да, grep |
| D-06 | `MockConsumer` и `Consumer` имеют разную арность конструктора | Высокая | Да, трассировка |
| D-07 | `PendingCount` имеет несовместимую семантику в mock и real | Высокая | Да, трассировка |
| D-08 | Дублированная сборка `api_app`/`worker_app` с расхождениями | Высокая | Да, трассировка |
| D-09 | `ports.BillingClient.BlockBatch` объявлен, но не вызывается | Средняя | Да, grep |
| D-10 | `ports.AuthClient.GetUserInfo` объявлен, но не вызывается | Средняя | Да, grep |
| D-11 | `NewRouter` — 2 соседних `string` и 3 соседних `bool` позиционно | Средняя (риск) | Да, сигнатура |
| D-12 | `Handlers` смешивает usecase и сырые адаптеры в одной структуре | Средняя | Да, трассировка |
| D-13 | Схема событий Kafka не версионирована — продюсер и консьюмер связаны неявно | Средняя | Да, трассировка |
| D-14 | `ports.RevisionStore` смешивает чтение рантайма и админ-запись в один интерфейс | Низкая | Да, сигнатура |
| — | Чистота домена и usecase от транспортных типов | Не дефект | Проверено |

---

## D-01. Дрейф сигнатуры `NewConsumer` — HEAD не компилируется

**Суть.** Коммит «Kafka DLQ» добавил четвёртый параметр в конструктор `kafka.NewConsumer`, обновил одну точку вызова из двух и оставил вторую сломанной. Ветка `codebase-analysis` не собирается.

**Место.**
- `internal/adapters/kafka/consumer.go:31` — новая сигнатура с 4 параметрами
- `internal/app/api_app.go:219` — вызов с 3 аргументами (сломан)
- `internal/app/worker_app.go:133` — вызов с 4 аргументами (обновлён)

**Подробное описание.**

Конструктор после коммита DLQ:

```go
// internal/adapters/kafka/consumer.go:31
func NewConsumer(
	brokers []string,
	groupID string,
	handler MessageHandler,
	dlq DLQProducer,      // <-- добавлен
) *Consumer
```

Точка вызова в worker обновлена корректно, точка вызова в API — нет:

```go
// internal/app/api_app.go:219
bulkConsumer := kafkaadapter.NewConsumer(
	cfg.Kafka.Brokers,
	cfg.Kafka.BulkGroupID,
	bulkHandler,
	// четвёртый аргумент отсутствует
)
```

**Трассировка.**

```
$ go build ./...
internal/app/api_app.go:219:48: not enough arguments in call to kafkaadapter.NewConsumer
	have ([]string, string, *kafkatransport.BulkJobHandler)
	want ([]string, string, kafkaadapter.MessageHandler, kafkaadapter.DLQProducer)

FAIL github.com/ikermy/BFF/cmd/api      [build failed]
FAIL github.com/ikermy/BFF/cmd/worker   [build failed]
FAIL github.com/ikermy/BFF/internal/app [build failed]
```

Падают оба бинаря и пакет сборки. Пакеты `internal/usecase`, `internal/transport/...`, `internal/adapters/...` собираются и тестируются нормально — именно поэтому дефект не был замечен: `go test ./internal/usecase/...` зелёный, а `make test` (см. F-19) не вызывает `go build ./...`.

**Почему это дефект границы, а не опечатка.** Порт `MessageHandler` объявлен в `internal/ports/ports.go`, но `DLQProducer` — в пакете адаптера `internal/adapters/kafka`. Конструктор адаптера принимает тип, объявленный в том же адаптере, вместо порта. Добавление зависимости в адаптер потребовало правки всех точек сборки, и компилятор поймал это только в `internal/app` — пакете, который не покрыт тестами (`internal/app` не имеет ни одного `_test.go`).

**Предлагаемый багфикс.**

```go
// internal/app/api_app.go:219
bulkConsumer := kafkaadapter.NewConsumer(
	cfg.Kafka.Brokers,
	cfg.Kafka.BulkGroupID,
	bulkHandler,
	kafkaProducer, // DLQ-продюсер, уже создан выше в этой функции
)
```

Сопутствующие меры, чтобы дефект не повторился:

1. Добавить в `Makefile` цель, которая реально собирает всё (см. F-19):
   ```makefile
   build:
   	go build ./...
   check: build vet test
   ```
2. Перенести `DLQProducer` в `internal/ports`, чтобы адаптер зависел от порта, а не от собственного типа.
3. Ввести smoke-тест на сборку графа зависимостей — минимальный тест в `internal/app`, вызывающий `BuildAPIAppWithContext` с mock-конфигом. Это единственный пакет без тестов, и именно он сломался.

**Комментарий.** Дефект блокирует проверку любых других исправлений: пока `go build ./...` красный, ни один фикс из A, B, C, E нельзя ни собрать, ни прогнать в интеграционных тестах. Это первый пункт на исправление во всём аудите. Дублируется как F-01 в разделе эксплуатации — там он рассматривается как отсутствие CI, здесь как дрейф контракта.

---

## D-02. Реальные адаптеры никогда не возвращают `*domain.AppError` — коды ошибок теряются на границе

**Суть.** Домен определяет богатую систему кодов (`INSUFFICIENT_FUNDS`, `BARCODEGEN_ERROR`, `PARTIAL_FUNDS` и др.) с привязкой к HTTP-статусам, но ни один реальный HTTP-адаптер их не производит. Все ошибки внешних сервисов возвращаются как `fmt.Errorf` и на выходе схлопываются в `500 INTERNAL_ERROR`.

**Место.**
- `internal/domain/apperrors.go` — определение `AppError{Code, HTTPStatus, Message, Details}`
- `internal/adapters/billing/http_client.go:188,193,202,206-208` — только `fmt.Errorf`
- `internal/adapters/barcodegen/http_client.go` — только `fmt.Errorf`
- `internal/adapters/history/http_client.go` — только `fmt.Errorf`
- `internal/adapters/ai/http_client.go` — только `fmt.Errorf`
- `internal/transport/http/gin/error_handler.go` — маппинг `*domain.AppError` → HTTP

**Подробное описание.**

Единственная точка формирования ошибки в реальном биллинг-клиенте:

```go
// internal/adapters/billing/http_client.go:206-208
if resp.StatusCode < 200 || resp.StatusCode >= 300 {
	errBody, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("billing: %s: status %d: %s", path, resp.StatusCode, string(errBody))
}
```

HTTP-статус, который несёт семантику (402 — нет средств, 409 — конфликт саги, 429 — троттлинг), превращается в строку внутри текста ошибки. Структурная информация теряется безвозвратно.

Обработчик ошибок на выходе умеет работать только с типизированной ошибкой:

```go
// internal/transport/http/gin/error_handler.go
var appErr *domain.AppError
if errors.As(err, &appErr) {
	c.JSON(appErr.HTTPStatus, gin.H{"code": appErr.Code, ...})
	return
}
// иначе
c.JSON(500, gin.H{"code": "INTERNAL_ERROR", ...})
```

Поскольку `errors.As` никогда не срабатывает для ошибок реальных адаптеров, любой сбой Billing/BarcodeGen/AI/History отдаётся клиенту как `500`.

**Трассировка.**

Сценарий: у пользователя недостаточно средств, Billing отвечает `402 Payment Required`.

```
1. usecase вызывает u.billing.Block(ctx, req)
2. billing/http_client.go:206 — StatusCode == 402, не в диапазоне 2xx
3. → fmt.Errorf("billing: /block: status 402: {\"error\":\"insufficient funds\"}")
4. usecase возвращает эту ошибку наверх без обёртки
5. error_handler: errors.As(err, &appErr) == false
6. Клиент получает: 500 {"code":"INTERNAL_ERROR"}
```

Ожидалось: `402 {"code":"INSUFFICIENT_FUNDS"}` — код существует в домене и описан в ТЗ.

Побочные следствия:

- **Клиент не может отличить свою ошибку от аварии сервиса.** `402` требует пополнить кошелёк, `500` — повторить позже. Оба выглядят как `500`.
- **Ретраи на стороне клиента вредны.** Клиент, увидев `500`, повторит запрос, который гарантированно провалится снова.
- **Текст внешнего сервиса утекает клиенту** (E-16): тело ответа Billing попадает в сообщение ошибки.
- **Метрики бесполезны** (F-15): все внешние сбои в одной корзине `error`.

Где `AppError` всё-таки создаётся — только внутри usecase, синтетически:

```go
// internal/usecase/generate_usecase.go
if !quote.Allowed {
	return nil, domain.NewInsufficientFundsError(...)
}
```

То есть `INSUFFICIENT_FUNDS` возникает лишь тогда, когда usecase сам вычислил недостаток средств из **успешного** ответа `Quote`. Если Billing вернул `402` на этапе `Block`, код не появится.

**Предлагаемый багфикс.**

Ввести маппинг статуса в код на границе адаптера:

```go
// internal/adapters/billing/http_client.go
func (c *HTTPClient) mapError(path string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	switch resp.StatusCode {
	case http.StatusPaymentRequired:
		return domain.NewAppError(
			domain.ErrCodeInsufficientFunds, http.StatusPaymentRequired,
			"insufficient funds", nil,
		)
	case http.StatusConflict:
		return domain.NewAppError(
			domain.ErrCodeSagaConflict, http.StatusConflict,
			"saga already exists", nil,
		)
	case http.StatusTooManyRequests:
		return domain.NewAppError(
			domain.ErrCodeBillingThrottled, http.StatusServiceUnavailable,
			"billing throttled", nil,
		).AsRetryable()
	}

	if resp.StatusCode >= 500 {
		return domain.NewAppError(
			domain.ErrCodeBillingError, http.StatusBadGateway,
			"billing unavailable", nil,
		).AsRetryable() // см. C-07: типизированный признак вместо разбора строки
	}

	// 4xx — не ретраить, детали только в лог
	log.Printf("billing: %s: status %d: %s", path, resp.StatusCode, body)
	return domain.NewAppError(
		domain.ErrCodeBillingError, http.StatusBadGateway,
		"billing rejected request", nil,
	)
}
```

Тело внешнего сервиса уходит в лог, а не клиенту — закрывает E-16 в той же правке. Аналогично для BarcodeGen, AI, History.

**Комментарий.** Это корневая причина сразу нескольких дефектов из других разделов: C-07 (классификация ретраев по подстроке существует только потому, что структурной ошибки нет), E-16 (утечка текста внешнего сервиса), F-15 (метрики не различают 4xx/5xx). Исправление здесь удаляет необходимость в костылях там. Порты объявлены как возвращающие `error`, что синтаксически допускает `*domain.AppError`, — менять сигнатуры портов не нужно, достаточно начать возвращать типизированную ошибку.

---

## D-03. Mock и real возвращают несовместимые формы ошибок — retry работает только в прод

**Суть.** Классификация ретраев в usecase разбирает текст ошибки на подстроку `status 5`. Реальные адаптеры такой текст формируют, mock-адаптеры — нет. Поведение retry в dev-профиле и в прод-профиле принципиально различается, а тесты, использующие mock, не могут покрыть retry-путь.

**Место.**
- `internal/usecase/generate_usecase.go` — `isRetryableError` через `strings.Contains`
- `internal/adapters/billing/http_client.go:208` — формат `"billing: %s: status %d: %s"`
- `internal/adapters/billing/mock_client.go:77,81,89,97,105` — формат без статуса
- `internal/adapters/barcodegen/mock_client.go:28,34` — проброс `err` без обёртки

**Подробное описание.**

Реальный клиент включает статус в текст:

```go
// internal/adapters/billing/http_client.go:208
return fmt.Errorf("billing: %s: status %d: %s", path, resp.StatusCode, string(errBody))
// → "billing: /block: status 503: upstream timeout"
```

Mock — нет:

```go
// internal/adapters/billing/mock_client.go:77-105
return fmt.Errorf("sagaID is required for block")
return fmt.Errorf("units must be >= 0")
return fmt.Errorf("invalid capture request")
return fmt.Errorf("invalid release request")
return nil, fmt.Errorf("invalid block-batch request")
```

Классификатор в usecase (подробно разобран как C-07):

```go
func isRetryableError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "status 500") ||
		strings.Contains(msg, "status 502") ||
		strings.Contains(msg, "status 503") ||
		// ...
}
```

**Трассировка.**

Прод-профиль (реальный BarcodeGen отдаёт 503):

```
1. barcodegen/http_client.go → fmt.Errorf("... status 503 ...")
2. isRetryableError → strings.Contains(msg, "status 503") == true
3. Retry: попытка 2 через 1с, попытка 3 через 3с
```

Dev-профиль (mock BarcodeGen настроен на сбой):

```
1. barcodegen/mock_client.go:28 → return domain.BarcodeItem{}, err
   где err — заранее заданная ошибка вида errors.New("mock failure")
2. isRetryableError → ни одна подстрока не совпала → false
3. Retry не выполняется, сага сразу идёт в Release
```

Следствия:

1. **Retry-путь непокрываем тестами через mock.** Чтобы протестировать retry, тест обязан подделать текст ошибки строкой `"status 503"` — то есть тест проверяет формат сообщения реального HTTP-клиента, а не логику. Это связывает тест usecase с деталью реализации адаптера транспорта.
2. **Dev ведёт себя не как прод.** Отладка сценариев отказа на mock даёт ложную картину: в dev сага падает сразу, в прод — после трёх попыток и 94 секунд (см. A-01, расчёт бюджета).
3. **Порт не документирует контракт ошибок.** `ports.BillingClient` объявляет `error`, но негласно требует «текст должен содержать `status NNN`». Ни один mock об этом требовании не знает.

**Предлагаемый багфикс.**

Ввести типизированный признак ретраебельности в домене — это одновременно закрывает C-07 и D-02:

```go
// internal/domain/apperrors.go
type AppError struct {
	Code       string
	HTTPStatus int
	Message    string
	Details    map[string]any
	Retryable  bool // новое поле
}

func (e *AppError) AsRetryable() *AppError {
	e.Retryable = true
	return e
}

// IsRetryable — единственная точка истины для всех адаптеров.
func IsRetryable(err error) bool {
	var appErr *AppError
	if errors.As(err, &appErr) {
		return appErr.Retryable
	}
	// сетевые ошибки транспорта
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	return false
}
```

В usecase:

```go
// было: strings.Contains(msg, "status 500") || ...
if !domain.IsRetryable(err) {
	break
}
```

Mock-адаптеры получают возможность выражать ретраебельность честно:

```go
// internal/adapters/barcodegen/mock_client.go
if c.FailWithRetryable {
	return domain.BarcodeItem{}, domain.NewAppError(
		domain.ErrCodeBarcodeGenError, http.StatusBadGateway, "mock 503", nil,
	).AsRetryable()
}
```

Дополнительно — контрактные тесты, прогоняющие один и тот же набор проверок против mock и real (real — против `httptest.Server`):

```go
// internal/adapters/billing/contract_test.go
func testBillingContract(t *testing.T, newClient func(url string) ports.BillingClient) {
	t.Run("5xx is retryable", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(503)
		}))
		defer srv.Close()
		err := newClient(srv.URL).Block(context.Background(), validReq())
		if !domain.IsRetryable(err) {
			t.Fatal("5xx must be retryable")
		}
	})
	// 402 → INSUFFICIENT_FUNDS, 4xx → не retryable, и т.д.
}
```

**Комментарий.** Расхождение mock и real — самый недооценённый класс дефектов в этом проекте: mock-адаптеры используются не только в тестах, но и как рантайм-фолбэк при отсутствии ENV (см. E-04, E-05, E-06). Значит, различие форм ошибок проявляется не только в тестах, но и в работающем сервисе, если забыть переменную окружения. Единая типизированная ошибка устраняет весь класс.

---

## D-04. Обход слоёв: `pdf417`/`code128` дергают адаптер напрямую, минуя usecase и биллинг

**Суть.** Два публичных эндпоинта вызывают `ports.BarcodeGenClient` прямо из HTTP-обработчика, минуя `GenerateUseCase`. В результате платный результат выдаётся без котировки, блокировки и списания средств.

**Место.**
- `internal/transport/http/gin/api_handler.go` — `h.barcode.GeneratePDF417(...)`, `h.barcode.GenerateCode128(...)`
- `internal/transport/http/gin/router.go` — регистрация `POST /api/v1/barcode/generate/pdf417`, `/code128`
- `internal/usecase/generate_usecase.go` — сага, которую эти пути не проходят

**Подробное описание.**

Структура обработчика держит и usecase, и сырой адаптер:

```go
type APIHandler struct {
	generate *usecase.GenerateUseCase
	edit     *usecase.EditUseCase
	quote    *usecase.QuoteUseCase
	bulk     *usecase.BulkUseCase
	barcode  ports.BarcodeGenClient // прямой доступ к платному сервису
	history  ports.HistoryClient
	consumer ports.BulkConsumer
}
```

Обработчик «сырой» генерации:

```go
// internal/transport/http/gin/api_handler.go
// Не проходит через billing — прямой проброс в BarcodeGen
func (h *APIHandler) GeneratePDF417(c *gin.Context) {
	// ...
	item, err := h.barcode.GeneratePDF417(c.Request.Context(), req)
	// ...
}
```

Комментарий в коде утверждает, что так задумано. Однако сравнение маршрутов показывает несоответствие:

| Маршрут | Auth | Idempotency | Quote | Block | Capture |
|---|---|---|---|---|---|
| `POST /barcode/generate` | User JWT | да | да | да | да |
| `POST /barcode/generate/pdf417` | User JWT | да | **нет** | **нет** | **нет** |
| `POST /barcode/generate/code128` | User JWT | да | **нет** | **нет** | **нет** |

**Трассировка.**

```
1. Клиент с валидным User JWT → POST /api/v1/barcode/generate/pdf417
2. UserJWTMiddleware: токен валиден, userID извлечён
3. IdempotencyMiddleware: ключ зарезервирован
4. APIHandler.GeneratePDF417
5. h.barcode.GeneratePDF417(ctx, req) — прямой HTTP в BarcodeGen
6. 200 OK с готовым PDF417
   Списано средств: 0
   Записей в History: 0
   Событий Kafka: 0
```

Тот же результат, что и через `POST /barcode/generate`, но бесплатно и без следа в истории.

Дополнительные следствия:

- **Нет записи в History** → баркод нельзя ни отредактировать, ни найти; но и нельзя предъявить при разборе инцидента.
- **Нет событий** → `TransHistory` и `Notifications` не узнают о выдаче.
- **Метрика `RequestsTotal`** учитывает запрос, но метрик саги нет вовсе (F-06), поэтому расхождение «запросов много, списаний мало» не видно на дашборде.

Если эндпоинты действительно должны существовать (например, для внутренних нужд), они находятся не в той группе: они смонтированы в `/api/v1` под User JWT, а не в `/internal` под сервисным токеном.

**Предлагаемый багфикс.**

Вариант 1 (предпочтительный) — убрать прямой доступ и провести через usecase:

```go
// internal/transport/http/gin/api_handler.go
type APIHandler struct {
	generate *usecase.GenerateUseCase
	edit     *usecase.EditUseCase
	quote    *usecase.QuoteUseCase
	bulk     *usecase.BulkUseCase
	history  ports.HistoryClient
	// barcode удалён — обработчик не имеет доступа к платному сервису
}

func (h *APIHandler) GeneratePDF417(c *gin.Context) {
	// ...
	// Тот же путь саги, но с фиксированным типом баркода.
	resp, err := h.generate.Execute(c.Request.Context(), domain.GenerateRequest{
		UserID:      userID,
		BarcodeType: domain.BarcodeTypePDF417,
		// ...
	})
}
```

Вариант 2 — если бесплатный «сырой» доступ нужен по бизнесу, перенести в `/internal` под сервисный токен и завести отдельную квоту:

```go
// internal/transport/http/gin/router.go
internal := r.Group("/internal")
internal.Use(ServiceJWTMiddleware(internalJWT))
{
	internal.POST("/barcode/raw/pdf417", h.Internal.GenerateRawPDF417)
}
```

Архитектурная страховка — запретить обработчикам доступ к платным портам структурно. Обработчик должен зависеть только от usecase:

```go
// внести в code review checklist или в go vet-подобную проверку:
// пакет internal/transport/http/gin не должен импортировать ports.BarcodeGenClient
```

**Комментарий.** Это самый прямой обход монетизации в проекте, и он не спрятан — комментарий в коде описывает поведение как намеренное. Возможно, эндпоинты создавались для отладки и остались в публичной группе. Требуется решение владельца продукта: удалить, перенести в `/internal` или провести через биллинг. До этого решения дыра остаётся открытой для любого пользователя с валидным токеном. Пересекается с E-01/E-02 (IDOR) по классу «публичная группа делает больше, чем предполагает её защита».

---

## D-05. Нет ни одной compile-time проверки соответствия адаптера порту

**Суть.** В проекте отсутствуют утверждения вида `var _ ports.X = (*Adapter)(nil)`. Соответствие адаптера порту проверяется только в точке сборки графа в `internal/app` — пакете, не имеющем тестов. Расхождение обнаруживается позже, чем могло бы.

**Место.**
- `internal/adapters/**` — ни в одном файле нет `var _ ports.<Interface> = ...`
- `internal/app/api_app.go`, `internal/app/worker_app.go` — единственные места, где соответствие проверяется компилятором

**Подробное описание.**

Проверка присутствия утверждений:

```
$ grep -rn "var _ " --include=*.go internal/ | grep -v _test.go
(пусто)
```

В Go идиоматично закреплять реализацию интерфейса рядом с типом:

```go
var _ ports.BillingClient = (*HTTPClient)(nil)
```

Без этого компилятор узнаёт о несоответствии только там, где адаптер передаётся в функцию, ожидающую порт, — то есть в `internal/app`. У этого пакета есть две особенности, усиливающие проблему:

1. **Ни одного `_test.go`** — расхождение не поймает `go test ./internal/...`.
2. **Дублированная сборка** (D-08) — `api_app.go` и `worker_app.go` собирают разные подмножества адаптеров, поэтому адаптер, используемый только в worker, вообще не проверяется при сборке API, и наоборот.

Именно так прошёл D-01: `internal/usecase` и `internal/adapters` собирались и тестировались, а сломанным оказался единственный непокрытый пакет.

**Трассировка.**

Сценарий, который сейчас не будет обнаружен рано: разработчик добавляет метод в порт.

```
1. В ports.HistoryClient добавлен метод DeleteBarcode(ctx, id) error
2. Реализован в history/http_client.go
3. Забыт в history/mock_client.go
4. go build ./internal/adapters/... → OK (mock ни к чему не приводится)
5. go test ./internal/usecase/... → OK
6. go build ./internal/app/... → ошибка, но только если mock
   действительно передаётся в этой ветке сборки
7. Если mock выбирается только при отсутствии ENV (E-04),
   ошибка возникает в рантайме конкретной конфигурации
```

**Предлагаемый багфикс.**

Добавить утверждения во все адаптеры — 15 портов, около 25 реализаций:

```go
// internal/adapters/billing/http_client.go
var _ ports.BillingClient = (*HTTPClient)(nil)

// internal/adapters/billing/mock_client.go
var _ ports.BillingClient = (*MockClient)(nil)

// internal/adapters/barcodegen/http_client.go
var _ ports.BarcodeGenClient = (*HTTPClient)(nil)

// internal/adapters/barcodegen/mock_client.go
var _ ports.BarcodeGenClient = (*MockClient)(nil)

// internal/adapters/history/http_client.go
var _ ports.HistoryClient = (*HTTPClient)(nil)

// internal/adapters/history/mock_client.go
var _ ports.HistoryClient = (*MockClient)(nil)

// internal/adapters/ai/http_client.go
var _ ports.AIClient = (*HTTPClient)(nil)

// internal/adapters/ai/mock_client.go
var _ ports.AIClient = (*MockClient)(nil)

// internal/adapters/auth/http_client.go
var _ ports.AuthClient = (*HTTPClient)(nil)

// internal/adapters/auth/local_validator.go
var _ ports.AuthClient = (*LocalValidator)(nil)

// internal/adapters/auth/mock_client.go
var _ ports.AuthClient = (*MockClient)(nil)

// internal/adapters/events/kafka_publisher.go
var _ ports.EventPublisher = (*KafkaPublisher)(nil)

// internal/adapters/events/mock_publisher.go
var _ ports.EventPublisher = (*MockPublisher)(nil)

// internal/adapters/idempotency/redis_store.go
var _ ports.IdempotencyStore = (*RedisStore)(nil)

// internal/adapters/idempotency/memory_store.go
var _ ports.IdempotencyStore = (*MemoryStore)(nil)

// internal/adapters/idempotency/redis_locker.go
var _ ports.Locker = (*RedisLocker)(nil)

// internal/adapters/idempotency/memory_locker.go
var _ ports.Locker = (*MemoryLocker)(nil)

// internal/adapters/revisions/memory_store.go
var _ ports.RevisionStore = (*MemoryStore)(nil)

// internal/adapters/timeouts/memory_store.go
var _ ports.TimeoutStore = (*MemoryStore)(nil)

// internal/adapters/topupbonus/memory_store.go
var _ ports.TopupBonusStore = (*MemoryStore)(nil)

// internal/adapters/kafka/consumer.go
var _ ports.BulkConsumer = (*Consumer)(nil)

// internal/adapters/kafka/mock_consumer.go
var _ ports.BulkConsumer = (*MockConsumer)(nil)
```

Плюс минимальный smoke-тест на сборку графа, чтобы `internal/app` перестал быть непокрытым:

```go
// internal/app/api_app_test.go
func TestBuildAPIAppWithMockConfig(t *testing.T) {
	// Все ENV пусты → выбираются mock-адаптеры.
	app, err := BuildAPIAppWithContext(context.Background(), testConfig())
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
}
```

**Комментарий.** Дешёвая правка с высокой отдачей: 22 строки закрывают целый класс поздних обнаружений и делают явным, какой адаптер какой порт реализует (сейчас это выясняется только чтением `internal/app`). Строго говоря, это не дефект поведения, а отсутствующая страховка, но именно её отсутствие позволило D-01 доехать до HEAD.

---

## D-06. `MockConsumer` и `Consumer` имеют разную арность конструктора

**Суть.** Реальный и mock consumer реализуют один порт `ports.BulkConsumer`, но их конструкторы принимают разное число аргументов. Замена одного на другой требует правки кода сборки, а не только выбора реализации.

**Место.**
- `internal/adapters/kafka/consumer.go:31` — `NewConsumer(brokers, groupID, handler, dlq)`
- `internal/adapters/kafka/mock_consumer.go` — `NewMockConsumer(...)` с иной арностью
- `internal/app/api_app.go:219`, `internal/app/worker_app.go:133` — точки выбора

**Подробное описание.**

Порт скрывает различие:

```go
// internal/ports/ports.go
type BulkConsumer interface {
	Start(ctx context.Context) error
	PendingCount(ctx context.Context) (int, error)
	Close() error
}
```

Но конструкторы несимметричны: реальный требует список брокеров, groupID и DLQ-продюсера, mock — ничего из этого. В коде сборки это выражается ветвлением, где каждая ветка вызывает свой конструктор со своим набором аргументов. Добавление параметра в один конструктор не отражается на другом — компилятор не связывает их между собой.

**Трассировка.**

Именно эта асимметрия усилила D-01:

```
1. Коммит DLQ добавляет параметр dlq в NewConsumer
2. NewMockConsumer не меняется — mock не умеет в DLQ
3. worker_app.go:133 обновлён (реальная ветка)
4. api_app.go:219 не обновлён
5. Компилятор не мог подсказать симметрию: у mock другая форма вызова,
   сравнивать не с чем
```

Дополнительно: `MockConsumer` не принимает DLQ вовсе, значит DLQ-путь (публикация сообщения в `bulk.tasks.dlq` при ошибке обработчика) в dev-профиле не существует. Тестировать поведение DLQ на mock нельзя — см. также B-10 (в DLQ уходит весь батч).

**Предлагаемый багфикс.**

Привести конструкторы к общей форме через структуру опций:

```go
// internal/adapters/kafka/consumer.go
type ConsumerConfig struct {
	Brokers []string
	GroupID string
	Handler ports.MessageHandler
	DLQ     ports.DLQProducer // порт, а не тип адаптера (см. D-01)
}

func NewConsumer(cfg ConsumerConfig) *Consumer { ... }
```

```go
// internal/adapters/kafka/mock_consumer.go
// Тот же тип конфигурации: mock игнорирует Brokers/GroupID,
// но принимает Handler и DLQ, чтобы воспроизводить прод-путь.
func NewMockConsumer(cfg ConsumerConfig) *MockConsumer { ... }
```

Тогда точка сборки становится симметричной, и добавление поля в `ConsumerConfig` автоматически доступно обеим реализациям без правки точек вызова:

```go
// internal/app/api_app.go
consumerCfg := kafkaadapter.ConsumerConfig{
	Brokers: cfg.Kafka.Brokers,
	GroupID: cfg.Kafka.BulkGroupID,
	Handler: bulkHandler,
	DLQ:     kafkaProducer,
}

var bulkConsumer ports.BulkConsumer
if len(cfg.Kafka.Brokers) == 0 {
	bulkConsumer = kafkaadapter.NewMockConsumer(consumerCfg)
} else {
	bulkConsumer = kafkaadapter.NewConsumer(consumerCfg)
}
```

**Комментарий.** Структура опций здесь предпочтительнее выравнивания позиционных параметров: у consumer уже 4 зависимости, и список будет расти (retry-топик, backoff, лимит попыток). Дополнительный выигрыш — `MockConsumer`, принимающий DLQ, делает DLQ-путь тестируемым, что нужно для проверки исправлений B-06 и B-10.

---

## D-07. `PendingCount` имеет несовместимую семантику в mock и real

**Суть.** Один метод порта означает разное в двух реализациях: у mock — текущая длина внутренней очереди (идемпотентное чтение), у real — количество сообщений с момента предыдущего вызова (деструктивное чтение). Потребитель порта не может опираться ни на одну из семантик.

**Место.**
- `internal/ports/ports.go` — `PendingCount(ctx) (int, error)` без документации семантики
- `internal/adapters/kafka/consumer.go` — реализация через `reader.Stats().Lag`
- `internal/adapters/kafka/mock_consumer.go` — реализация через длину слайса
- `internal/transport/http/gin/api_handler.go` — `BulkWake`, единственный потребитель

**Подробное описание.**

Реальная реализация опирается на `kafka-go`:

```go
func (c *Consumer) PendingCount(ctx context.Context) (int, error) {
	return int(c.reader.Stats().Lag), nil
}
```

Документация `kafka-go` (проверено в `GOMODCACHE`, `reader.go`):

> Stats returns a snapshot of the reader stats **since the last time the method was called**.

То есть `Stats()` — деструктивное чтение со сбросом счётчиков. Два последовательных вызова дают разные результаты при неизменном состоянии топика. Кроме того, `Lag` осмыслен только если reader реально выполнял fetch, а в API-процессе `Start()` не вызывается никогда (F-07), поэтому значение практически всегда 0.

Mock-реализация возвращает длину внутренней очереди — идемпотентно и предсказуемо.

**Трассировка.**

```
Прод-профиль, живой consumer, 100 сообщений в топике:
1-й GET bulk/wake → Stats().Lag = 100  (счётчик сброшен)
2-й GET bulk/wake → Stats().Lag = 0    (с момента прошлого вызова ничего)
Вывод потребителя: «очередь пуста» — неверно

Прод-профиль, API-процесс (Start не вызван):
любой GET bulk/wake → Lag = 0 всегда

Dev-профиль, mock:
любой GET bulk/wake → len(queue), стабильно и корректно
```

Итог: эндпоинт, назначение которого — подтвердить, что BFF читает Kafka, в dev работает, в прод — нет. Ошибка не проявится ни в одном тесте, потому что тесты используют mock.

**Предлагаемый багфикс.**

Сначала зафиксировать контракт в порту:

```go
// internal/ports/ports.go
type BulkConsumer interface {
	Start(ctx context.Context) error

	// PendingCount возвращает текущее число необработанных сообщений
	// (consumer lag) на момент вызова. Вызов идемпотентен: повторный
	// вызов без изменения состояния топика возвращает то же значение.
	PendingCount(ctx context.Context) (int, error)

	Close() error
}
```

Затем привести реальную реализацию в соответствие — считать лаг неразрушающе, а не через `Stats()`:

```go
func (c *Consumer) PendingCount(ctx context.Context) (int, error) {
	if !c.started.Load() {
		return 0, domain.NewAppError(
			domain.ErrCodeConsumerNotStarted, http.StatusServiceUnavailable,
			"consumer is not running", nil,
		)
	}

	// Неразрушающий расчёт: последний оффсет партиции минус закоммиченный.
	lag, err := c.readLagFromOffsets(ctx)
	if err != nil {
		return 0, err
	}
	return lag, nil
}
```

Плюс контрактный тест, прогоняемый против обеих реализаций:

```go
func testBulkConsumerContract(t *testing.T, c ports.BulkConsumer) {
	t.Run("PendingCount is idempotent", func(t *testing.T) {
		first, err := c.PendingCount(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		second, err := c.PendingCount(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if first != second {
			t.Fatalf("PendingCount not idempotent: %d then %d", first, second)
		}
	})
}
```

Этот тест сейчас проходит на mock и падает на real — что и требуется зафиксировать.

**Комментарий.** Дефект показывает, чем опасен недокументированный порт: обе реализации «корректны» относительно подписи `(int, error)` и обе неверны относительно ожиданий потребителя. Пересекается с F-07 и F-08, где та же проблема рассмотрена со стороны наблюдаемости; здесь корень — отсутствие контракта в интерфейсе. Исправление имеет смысл делать вместе с F-07 (вызвать `Start()` либо убрать эндпоинт).

---

## D-08. Дублированная сборка `api_app`/`worker_app` с расхождениями

**Суть.** Два файла сборки графа зависимостей содержат почти идентичный код выбора адаптеров. Расхождения между ними не выявляются ни компилятором, ни тестами, и уже привели к D-01.

**Место.**
- `internal/app/api_app.go` — сборка API-процесса
- `internal/app/worker_app.go` — сборка worker-процесса
- Совпадающие фрагменты: выбор Billing, BarcodeGen, AI, History, Auth, EventPublisher, Redis

**Подробное описание.**

Оба файла реализуют одну и ту же логику «есть ENV-URL → реальный адаптер, иначе mock» для каждой из внешних зависимостей. Различие только в наборе транспортов: API поднимает HTTP-сервер, worker — Kafka-consumer.

Последствия дублирования, подтверждённые в коде:

1. **D-01** — сигнатура `NewConsumer` обновлена в `worker_app.go:133`, но не в `api_app.go:219`.
2. **API-процесс создаёт `bulkConsumer`, который никогда не стартует** (F-07). Ветка сборки скопирована из worker вместе с consumer, но `APIApp.Run` запускает только HTTP-сервер. Kafka-reader создаётся, занимает соединение и тот же `group.id`, что worker (A-04), и не используется.
3. **Мёртвый код в `api_app.go`** — результат `deferShutdown` присваивается в `_`, то есть shutdown-хук зарегистрирован и потерян.

Проверка, что `APIApp.Run` не запускает consumer:

```go
// internal/app/api_app.go
func (a *APIApp) Run() error {
	return a.server.Run() // bulkConsumer.Start() не вызывается
}
```

**Трассировка.**

```
Как расхождение возникает и почему не обнаруживается:
1. Правка нужна в 2 файлах, разработчик правит 1
2. go build ./internal/adapters/... → OK
3. go test ./internal/usecase/... → OK
4. internal/app не имеет тестов (D-05) → расхождение не поймано
5. make test не вызывает go build ./... (F-19) → красная сборка не видна
6. CI отсутствует (F-20) → доезжает до HEAD
```

**Предлагаемый багфикс.**

Вынести общую часть в один файл, оставив в `api_app.go`/`worker_app.go` только транспортную специфику:

```go
// internal/app/deps.go
// Deps — общий набор внешних зависимостей для обоих процессов.
type Deps struct {
	Billing    ports.BillingClient
	BarcodeGen ports.BarcodeGenClient
	AI         ports.AIClient
	History    ports.HistoryClient
	Auth       ports.AuthClient
	Events     ports.EventPublisher
	Idem       ports.IdempotencyStore
	Locker     ports.Locker
	Producer   ports.DLQProducer
}

// BuildDeps — единственное место выбора реальный/mock.
func BuildDeps(ctx context.Context, cfg config.Config) (*Deps, func(), error) {
	// ... вся логика выбора, ранее дублированная
}
```

```go
// internal/app/api_app.go
func BuildAPIAppWithContext(ctx context.Context, cfg config.Config) (*APIApp, error) {
	deps, cleanup, err := BuildDeps(ctx, cfg)
	if err != nil {
		return nil, err
	}
	// только HTTP-специфика: usecase → handlers → router → server
	// bulkConsumer здесь не создаётся вовсе
}
```

```go
// internal/app/worker_app.go
func BuildWorkerApp(ctx context.Context, cfg config.Config) (*WorkerApp, error) {
	deps, cleanup, err := BuildDeps(ctx, cfg)
	if err != nil {
		return nil, err
	}
	// только Kafka-специфика: handler → consumer
}
```

Заодно устраняется F-07/A-04 в части «API держит бесполезный reader с тем же group.id»: consumer перестаёт создаваться в API-процессе.

**Комментарий.** Дублирование само по себе стилистическое, но здесь оно стало механизмом доставки блокирующего дефекта. Правка ценна не сокращением строк, а тем, что делает невозможным расхождение конфигураций между процессами — сейчас нет никакой гарантии, что worker и API одинаково понимают, какой Billing использовать.

---

## D-09. `ports.BillingClient.BlockBatch` объявлен, но не вызывается

**Суть.** Метод присутствует в порту и реализован в обоих адаптерах, но ни одна точка продакшн-кода его не вызывает. Мёртвый контракт, который нужно поддерживать при каждом изменении порта.

**Место.**
- `internal/ports/ports.go` — объявление `BlockBatch`
- `internal/adapters/billing/http_client.go` — реализация
- `internal/adapters/billing/mock_client.go:105` — реализация

**Подробное описание.**

Проверка точек вызова:

```
$ grep -rn "\.BlockBatch(" --include=*.go internal/ cmd/ | grep -v _test.go | grep -v internal/ports/
(пусто)
```

Метод, вероятно, задумывался для bulk-пути: блокировать средства на весь батч одним вызовом вместо N отдельных. Фактически `BulkUseCase` обрабатывает элементы поштучно, вызывая обычный `Block` на каждый (что и порождает B-10 — реплей DLQ оплачивает уже успешные элементы заново).

Стоимость мёртвого метода: при добавлении поля в запрос блокировки его нужно провести через `BlockBatch` в двух адаптерах, хотя код не исполняется. Наличие метода также вводит в заблуждение — читающий предполагает, что батчевая блокировка используется.

**Предлагаемый багфикс.**

Вариант 1 — удалить из порта и обеих реализаций:

```go
// internal/ports/ports.go
type BillingClient interface {
	Quote(ctx context.Context, req domain.QuoteRequest) (domain.Quote, error)
	Block(ctx context.Context, req domain.BlockRequest) error
	Capture(ctx context.Context, req domain.CaptureRequest) error
	Release(ctx context.Context, req domain.ReleaseRequest) error
	CheckFreeEdit(ctx context.Context, userID, barcodeID string) (bool, error)
	// BlockBatch удалён — не использовался
}
```

Вариант 2 — начать использовать в bulk-пути, что заодно улучшает B-10: одна блокировка на батч даёт корректную единицу компенсации.

```go
// internal/usecase/bulk_usecase.go
// Блокировать весь батч разом, чтобы реплей DLQ не оплачивал повторно.
blocked, err := u.billing.BlockBatch(ctx, domain.BlockBatchRequest{
	SagaID: batchSagaID,
	Items:  itemRequests,
})
```

**Комментарий.** Рекомендую вариант 2, но как часть отдельной работы над bulk-путём (вместе с B-05, B-10). Если такая работа не планируется в ближайшее время, лучше удалить метод: мёртвый код в порту опаснее отсутствующего, потому что создаёт ложное представление о возможностях системы.

---

## D-10. `ports.AuthClient.GetUserInfo` объявлен, но не вызывается

**Суть.** Метод получения информации о пользователе присутствует в порту и во всех трёх реализациях `AuthClient`, но не вызывается нигде в продакшн-коде.

**Место.**
- `internal/ports/ports.go` — объявление `GetUserInfo`
- `internal/adapters/auth/http_client.go` — реализация
- `internal/adapters/auth/local_validator.go` — реализация
- `internal/adapters/auth/mock_client.go` — реализация

**Подробное описание.**

```
$ grep -rn "\.GetUserInfo(" --include=*.go internal/ cmd/ | grep -v _test.go | grep -v internal/ports/
(пусто)
```

Три реализации поддерживаются для метода, который не исполняется. Особенно заметно в `local_validator.go`, где `GetUserInfo` должен извлекать данные из claims JWT — то есть дублирует работу, уже сделанную в `ValidateToken`.

Показательно, что метод мог бы закрыть E-01/E-02 (IDOR): проверка владельца баркода требует сопоставления `userID` из токена с владельцем ресурса. Но реализованный механизм получения данных пользователя не задействован, а проверка владельца отсутствует.

**Предлагаемый багфикс.**

Удалить из порта и трёх реализаций:

```go
// internal/ports/ports.go
type AuthClient interface {
	ValidateToken(ctx context.Context, token string) (domain.UserClaims, error)
	// GetUserInfo удалён — не использовался
}
```

Если данные пользователя понадобятся (например, для уведомлений с именем), вернуть метод вместе с первым реальным потребителем, а не заранее.

**Комментарий.** Вместе с D-09 это 2 мёртвых метода из общего числа объявленных в портах. Немного, но каждый расширяет площадь поддержки на 2-3 реализации. Рекомендация одинаковая: удалять до появления потребителя. Отмечу, что оба мёртвых метода относятся к сценариям, которые в проекте реализованы наполовину (батчевый биллинг, проверка владельца) — то есть это следы незавершённых замыслов, а не случайный мусор.

---

## D-11. `NewRouter` — 2 соседних `string` и 3 соседних `bool` позиционно

**Суть.** Конструктор роутера принимает 8 позиционных аргументов, среди которых два подряд идущих `string` и три подряд идущих `bool`. Перестановка любых из них не будет замечена компилятором и приведёт к неверной конфигурации безопасности. На текущем HEAD порядок правильный — это латентный риск, а не активный дефект.

**Место.**
- `internal/transport/http/gin/router.go:22-31` — сигнатура
- `internal/app/api_app.go:228-237` — точка вызова

**Подробное описание.**

Сигнатура:

```go
// internal/transport/http/gin/router.go:22
func NewRouter(
	h Handlers,
	auth ports.AuthClient,
	idempotency ports.IdempotencyStore,
	internalJWT string,        // <-- 4
	adminJWT string,           // <-- 5
	enableLegacyAuth bool,     // <-- 6
	enableIdempotency bool,    // <-- 7
	maintenanceMode bool,      // <-- 8
) *gin.Engine
```

Точка вызова:

```go
// internal/app/api_app.go:228
router := gintransport.NewRouter(
	gintransport.Handlers{API: apiHandler, Internal: internalHandler, Admin: adminHandler},
	authClient,
	idempotencyStore,
	cfg.InternalServiceJWT,      // → internalJWT  ✓
	cfg.AdminJWT,                // → adminJWT     ✓
	cfg.Features.EnableLegacyAuth,  // → enableLegacyAuth   ✓
	cfg.Features.EnableIdempotency, // → enableIdempotency  ✓
	cfg.MaintenanceMode,            // → maintenanceMode    ✓
)
```

Проверено использование внутри тела: `internalJWT` уходит в `ServiceJWTMiddleware` на `:83` и `:113`, `adminJWT` — в `AdminJWTMiddleware` на `:90`. Соответствие корректное.

Тем не менее конструкция хрупкая. Гипотетическая перестановка аргументов 4 и 5 дала бы: админский токен защищает `/internal`, сервисный — `/admin`. Компилятор молчит (оба `string`), тесты, использующие одинаковые токены для admin и internal, тоже молчат. При том, что оба токена — статические shared secret с дефолтными значениями (E-07, E-08), обнаружить такую ошибку в рантайме было бы трудно.

Аналогично для трёх `bool`: перестановка `enableIdempotency` и `maintenanceMode` привела бы к отключению идемпотентности при включении maintenance-режима.

**Предлагаемый багфикс.**

Заменить позиционные параметры структурой конфигурации:

```go
// internal/transport/http/gin/router.go
type RouterConfig struct {
	Handlers    Handlers
	Auth        ports.AuthClient
	Idempotency ports.IdempotencyStore

	InternalJWT string
	AdminJWT    string

	EnableLegacyAuth  bool
	EnableIdempotency bool
	MaintenanceMode   bool
}

func NewRouter(cfg RouterConfig) *gin.Engine {
	r := gin.New()
	r.Use(MaintenanceModeMiddleware(cfg.MaintenanceMode))
	// ...
	bulkWake.Use(ServiceJWTMiddleware(cfg.InternalJWT))
	admin.Use(AdminJWTMiddleware(cfg.AdminJWT))
	// ...
}
```

Точка вызова становится самодокументируемой и устойчивой к перестановке:

```go
router := gintransport.NewRouter(gintransport.RouterConfig{
	Handlers:          gintransport.Handlers{API: apiHandler, Internal: internalHandler, Admin: adminHandler},
	Auth:              authClient,
	Idempotency:       idempotencyStore,
	InternalJWT:       cfg.InternalServiceJWT,
	AdminJWT:          cfg.AdminJWT,
	EnableLegacyAuth:  cfg.Features.EnableLegacyAuth,
	EnableIdempotency: cfg.Features.EnableIdempotency,
	MaintenanceMode:   cfg.MaintenanceMode,
})
```

Дополнительно стоит завести отдельные типы для токенов, чтобы перестановка стала ошибкой компиляции даже при позиционном вызове:

```go
type InternalToken string
type AdminToken string
```

**Комментарий.** Явно помечаю как риск, а не дефект: я проверил порядок и он верный, поэтому «багфикс» здесь — превентивный. Приоритет тем не менее не нулевой, потому что цена ошибки — обмен токенов между админской и сервисной группами, то есть эскалация привилегий. Правку логично делать вместе с E-07 (замена shared secret на JWT), поскольку она всё равно затрагивает эти параметры.

---

## D-12. `Handlers` смешивает usecase и сырые адаптеры в одной структуре

**Суть.** `APIHandler` держит одновременно четыре usecase и два сырых порта (`BarcodeGenClient`, `HistoryClient`), а также `BulkConsumer`. Наличие сырых портов в транспортном слое структурно разрешает обход бизнес-логики — чем D-04 и пользуется.

**Место.**
- `internal/transport/http/gin/api_handler.go` — определение `APIHandler`
- `internal/transport/http/gin/router.go` — `Handlers{API, Internal, Admin}`

**Подробное описание.**

```go
type APIHandler struct {
	generate *usecase.GenerateUseCase
	edit     *usecase.EditUseCase
	quote    *usecase.QuoteUseCase
	bulk     *usecase.BulkUseCase

	barcode  ports.BarcodeGenClient // платный сервис напрямую
	history  ports.HistoryClient    // хранилище напрямую
	consumer ports.BulkConsumer     // инфраструктура напрямую
}
```

Первые четыре поля — правильная зависимость транспорта от бизнес-логики. Последние три — прямой доступ к внешним системам, минуя её.

Фактические места использования сырых портов:

| Поле | Использование | Проблема |
|---|---|---|
| `barcode` | `GeneratePDF417`, `GenerateCode128` | D-04: обход биллинга |
| `history` | `GetBarcode` | E-01: нет проверки владельца |
| `consumer` | `BulkWake` | F-07: consumer не запущен |

Все три сырых порта участвуют в дефектах, и в каждом случае причина одна: логика, которая должна быть в usecase (проверка прав, списание, состояние consumer), оказалась в обработчике либо отсутствует вовсе.

Показательно, что `GetBarcode` через `history` напрямую — единственная причина, по которой IDOR (E-01) вообще возможен: если бы чтение шло через usecase, проверка владельца была бы там же, где она уже есть для edit.

**Предлагаемый багфикс.**

Убрать сырые порты из обработчика, добавив недостающие методы в usecase:

```go
// internal/transport/http/gin/api_handler.go
type APIHandler struct {
	generate *usecase.GenerateUseCase
	edit     *usecase.EditUseCase
	quote    *usecase.QuoteUseCase
	bulk     *usecase.BulkUseCase
	barcodes *usecase.BarcodeQueryUseCase // новый: чтение с проверкой владельца
}
```

```go
// internal/usecase/barcode_query_usecase.go
// GetForUser возвращает баркод только если он принадлежит userID.
func (u *BarcodeQueryUseCase) GetForUser(ctx context.Context, userID, barcodeID string) (domain.BarcodeRecord, error) {
	rec, err := u.history.GetBarcode(ctx, barcodeID)
	if err != nil {
		return domain.BarcodeRecord{}, err
	}
	if rec.UserID != userID {
		// 404, а не 403 — не раскрывать существование чужого ресурса
		return domain.BarcodeRecord{}, domain.NewNotFoundError("barcode not found")
	}
	return rec, nil
}
```

`BulkWake` либо убрать (F-07), либо перенести управление состоянием consumer в отдельный компонент, доступный процессу, который его действительно запускает.

**Комментарий.** Это архитектурный корень трёх дефектов из разных разделов (D-04, E-01, F-07). Правка не тривиальная — требует нового usecase — но она устраняет саму возможность обхода, вместо того чтобы латать каждый обход отдельно. Рекомендую делать её одним заходом с E-01/E-02, поскольку проверка владельца — центральная часть и того, и другого.

---

## D-13. Схема событий Kafka не версионирована — продюсер и консьюмер связаны неявно

**Суть.** События публикуются как JSON без поля версии и без идентификатора события. Продюсер (BFF) и консьюмеры (Notifications, TransHistory) связаны неявным контрактом, который нельзя изменить совместимо и нельзя проверить.

**Место.**
- `internal/domain/events.go` — определения структур событий
- `internal/adapters/events/kafka_publisher.go` — публикация
- `internal/adapters/kafka/producer.go` — запись в топик
- `internal/transport/kafka/bulk_job_handler.go` — разбор входящих сообщений

**Подробное описание.**

Структуры событий не содержат метаданных:

```
$ grep -n "EventID\|EventType\|Version" internal/domain/events.go
(совпадений нет)
```

Публикуемое сообщение — это сериализованная доменная структура целиком. Отсюда несколько проблем связанности:

1. **Нет `eventId`** — консьюмер не может дедуплицировать. При at-least-once доставке (B-06) и реплее DLQ (B-10) уведомление о выдаче баркода будет отправлено пользователю повторно. Подробно как B-11.
2. **Нет `version`** — переименование или удаление поля ломает консьюмеров без предупреждения. Совместимую эволюцию схемы выразить нечем.
3. **Нет ключа партиции** — порядок событий по пользователю не гарантирован; `barcode.edited` может прийти раньше `barcode.generated`.
4. **Доменная структура = wire-формат.** Изменение внутреннего поля домена автоматически меняет внешний контракт. Обратное тоже верно: требования внешнего консьюмера просачиваются в домен.

Асимметрия входящего и исходящего направления: для входящих сообщений (`bulk.tasks`) в структуре есть `CorrelationID`, для исходящих событий метаданных нет вовсе. Причём `CorrelationID` не читается (F-04), то есть поле объявлено, но не используется.

**Предлагаемый багфикс.**

Ввести конверт события, отделив wire-формат от домена:

```go
// internal/domain/events.go
// Envelope — внешний контракт. Меняется только совместимо.
type Envelope struct {
	EventID     string          `json:"eventId"`     // UUID, для дедупликации
	EventType   string          `json:"eventType"`   // "barcode.generated"
	Version     int             `json:"version"`     // 1
	OccurredAt  time.Time       `json:"occurredAt"`
	TraceID     string          `json:"traceId"`     // см. F-04
	AggregateID string          `json:"aggregateId"` // userID — ключ партиции
	Payload     json.RawMessage `json:"payload"`     // доменные данные
}
```

```go
// internal/adapters/events/kafka_publisher.go
func (p *KafkaPublisher) publish(ctx context.Context, eventType string, aggregateID string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	env := domain.Envelope{
		EventID:     uuid.NewString(),
		EventType:   eventType,
		Version:     1,
		OccurredAt:  time.Now().UTC(),
		TraceID:     tracing.FromContext(ctx),
		AggregateID: aggregateID,
		Payload:     body,
	}

	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}

	// Ключ партиции = aggregateID: порядок событий по пользователю сохранён.
	return p.producer.Publish(ctx, p.topic, []byte(aggregateID), raw)
}
```

Изменение требует согласования с командами Notifications и TransHistory. Совместимый путь миграции — публиковать оба формата в переходный период либо добавить только новые поля (`eventId`, `version`, `traceId`) на верхний уровень существующей структуры, не перенося payload вглубь: консьюмеры проигнорируют неизвестные поля, а дедупликация станет возможной сразу.

**Комментарий.** Дополняет B-11, где та же проблема рассмотрена со стороны exactly-once. Здесь акцент на связанности: сейчас доменная структура и внешний контракт — один объект, поэтому любое изменение домена является ломающим изменением интеграции. Минимальный полезный шаг, не требующий согласования, — добавить `eventId` и `version` как дополнительные поля: это ничего не ломает у консьюмеров и открывает им возможность дедуплицировать.

---

## D-14. `ports.RevisionStore` смешивает чтение рантайма и админ-запись в один интерфейс

**Суть.** Один порт обслуживает и горячий путь генерации (чтение конфигурации ревизии), и админский путь управления (запись, листинг). Потребители получают доступ к операциям, которые им не нужны.

**Место.**
- `internal/ports/ports.go` — `RevisionStore` с методами чтения и записи
- `internal/usecase/generate_usecase.go` — потребитель, которому нужно только чтение
- `internal/transport/http/gin/admin_handler.go` — потребитель, которому нужна запись

**Подробное описание.**

Интерфейс объединяет две несвязанные ответственности: `Get`/`GetSchema` вызываются на каждой генерации, `List`/`Set` — только из админского API. В результате `GenerateUseCase` располагает методом записи конфигурации, а админский обработчик — методами горячего чтения.

Практическое следствие проявляется в A-09: запись конфигурации выполняет `os.WriteFile` под тем же мьютексом, который читается на каждом вызове горячего пути. Разделение интерфейсов не устранило бы блокировку само по себе, но сделало бы очевидным, что операции имеют разные характеристики (частота, стоимость, требования к консистентности).

**Предлагаемый багфикс.**

Разделить по потребителю:

```go
// internal/ports/ports.go

// RevisionReader — горячий путь. Только чтение.
type RevisionReader interface {
	Get(ctx context.Context, revisionID string) (domain.RevisionConfig, error)
	GetSchema(ctx context.Context, revisionID string) (domain.RevisionSchema, error)
}

// RevisionAdmin — админский путь. Чтение и запись.
type RevisionAdmin interface {
	RevisionReader
	List(ctx context.Context) ([]domain.RevisionConfig, error)
	Set(ctx context.Context, cfg domain.RevisionConfig) error
}
```

Один и тот же адаптер реализует оба интерфейса; потребители объявляют минимально необходимый:

```go
// internal/usecase/generate_usecase.go
type GenerateUseCase struct {
	revisions ports.RevisionReader // запись недоступна структурно
	// ...
}
```

**Комментарий.** Низкий приоритет: поведенческого дефекта нет, это вопрос гигиены интерфейсов. Включаю в отчёт, потому что тот же принцип (минимальный интерфейс у потребителя) устраняет D-04 и D-12 структурно — обработчик, объявляющий только нужные ему зависимости, физически не может обойти биллинг. Здесь этот принцип виден в наиболее безобидной форме, но природа та же.

---

## Что проверено и дефектом не является

**Чистота домена от транспортных типов.** Проверено:

```
$ grep -rn "gin-gonic\|segmentio\|net/http" --include=*.go internal/usecase/ internal/domain/ | grep -v _test.go
(пусто)
```

Ни `gin.Context`, ни типы `kafka-go`, ни `net/http` не проникают в `internal/usecase` и `internal/domain`. Для проекта, где границы соблюдаются вручную, без линтера архитектурных зависимостей, это заметный результат. Обратное направление тоже чисто: `internal/domain` импортирует только stdlib.

**Отсутствие импортов адаптеров в usecase.** Проверено:

```
$ grep -rn "internal/adapters\|internal/transport" --include=*.go internal/usecase/ internal/domain/ | grep -v _test.go
(пусто)
```

`internal/usecase` зависит исключительно от `internal/ports` и `internal/domain`. Инверсия зависимостей соблюдена: ни один usecase не знает о существовании конкретного адаптера.

**Полнота реализаций портов.** Все mock-адаптеры реализуют полный набор методов своих портов — расхождений «метод есть в real, отсутствует в mock» не обнаружено. Это подтверждается тем, что `internal/adapters/...` и `internal/usecase/...` собираются (падает только `internal/app` по D-01). Отмечу, что гарантии этому нет: без compile-time утверждений (D-05) полнота держится на том, что каждый адаптер где-то приводится к порту в коде сборки.

**Единственность определения интерфейсов.** Проверено, что интерфейсы объявлены только в `internal/ports/ports.go`:

```
$ grep -rn "^type .* interface {" --include=*.go internal/ | grep -v "internal/ports/"
```

Найденные вне `ports` интерфейсы относятся к внутренним деталям адаптеров (`MessageHandler`, `DLQProducer` в `internal/adapters/kafka`) — они не являются портами домена. Замечание по `DLQProducer` вынесено в D-01: его место в `internal/ports`, поскольку он участвует в конструкторе, вызываемом из слоя сборки.

**Именование и группировка портов.** 15 портов разделены по внешним системам согласно ответственности (Billing, BarcodeGen, AI, History, Auth, Events, Idempotency, Locker, RevisionStore, TimeoutStore, TopupBonusStore, BulkConsumer и др.). Интерфейсы узкие: большинство содержит 1-5 методов. `BillingClient` с 6 методами — самый широкий, и это оправдано связанностью операций саги. Признаков «god interface» нет.

---

## Порядок исправления

Зависимости между дефектами раздела и связи с другими разделами:

| Шаг | Дефекты | Обоснование |
|---|---|---|
| 1 | **D-01** | Блокирует всё: без зелёной сборки нельзя проверить ни один фикс из A/B/C/E/F |
| 2 | **D-05** + smoke-тест `internal/app` | Дешёвая страховка, предотвращающая повторение D-01; нужна до дальнейших правок границ |
| 3 | **D-02 + D-03** | Одна правка: типизированные ошибки адаптеров. Закрывает C-07, E-16, частично F-15 |
| 4 | **D-04 + D-12** | Вместе с E-01/E-02: убрать сырые порты из обработчика и добавить проверку владельца |
| 5 | **D-08** | Единая сборка зависимостей; попутно устраняет F-07 и часть A-04 |
| 6 | **D-06 + D-07** | Симметричные конструкторы и контракт `PendingCount`; вместе с F-07 |
| 7 | **D-11** | Структура конфигурации роутера; вместе с E-07 (замена shared secret) |
| 8 | **D-13** | Требует согласования с командами-консьюмерами; минимальный шаг (`eventId`, `version`) можно сделать сразу |
| 9 | **D-09, D-10, D-14** | Гигиена: удалить мёртвые методы, разделить `RevisionStore` |

Ключевое наблюдение раздела: три дефекта (D-02, D-04, D-12) являются корневыми причинами дефектов, зафиксированных в других разделах как отдельные проблемы. Исправление границ устраняет необходимость в частных заплатках:

- **D-02** (типизированные ошибки) → снимает C-07 (разбор подстроки), E-16 (утечка текста), улучшает F-15 (метрики по классам ошибок)
- **D-04 + D-12** (убрать сырые порты) → снимает обход биллинга и делает E-01/E-02 невозможными структурно
- **D-08** (единая сборка) → снимает F-07 (лишний consumer в API) и часть A-04 (конкуренция за `group.id`)
