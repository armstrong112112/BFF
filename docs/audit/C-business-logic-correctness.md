# Раздел C. Корректность бизнес-логики и оркестрации саги

**Область:** `internal/usecase/` — `GenerateUseCase`, `QuoteUseCase`, `ChainExecutor`, `EditUseCase`.
**Метод:** трассировка по коду + исполняемые пробы (`internal/usecase/zzc_probe_test.go`, удалён после прогона).
**Дата:** 2026-08-02. **Ветка:** `codebase-analysis`.

Все пробы прогонялись на реальных `GenerateUseCase`/`ChainExecutor` со стабами портов, записывающими фактические аргументы вызовов. Вывод проб приведён дословно.

> **Восстановленный раздел.** Первая версия была утеряна до записи файла. Все находки перепроверены заново на текущем HEAD; номера строк актуальны.

## Сводка

| ID | Название | Severity | Статус |
|---|---|---|---|
| C-01 | N единиц генерируются с идентичными данными | Blocker | Подтверждено пробой |
| C-02 | `sagaID` детерминирован и коллидирует между запросами | Critical | Подтверждено пробой |
| C-03 | Ключ идемпотентности BarcodeGen пуст при пустом входном | Critical | Подтверждено пробой |
| C-04 | AI-подпись генерируется без запроса клиента | High | Подтверждено пробой |
| C-05 | `fullName` собирается как `"<nil> <nil>"` | High | Подтверждено пробой |
| C-06 | `PARTIAL_FUNDS` — успех, переданный через канал ошибки с HTTP 200 | High | Подтверждено пробой |
| C-07 | Retryable-классификация по подстроке в тексте ошибки | High | Подтверждено пробой |
| C-08 | Нет топологической сортировки цепочки — порядок в YAML определяет успех | High | Подтверждено пробой |
| C-09 | Цепочка не выполняется при пустом `Fields` | Medium | Подтверждено трассировкой |
| C-10 | `validateMinimumSet` проверяет вход до цепочки — вычисляемые поля нельзя объявить обязательными | Medium | Подтверждено трассировкой |
| C-11 | Сбой `Release` на partial-пути проглатывается | High | Подтверждено трассировкой |
| C-12 | Сбой `Capture` после успешной генерации теряет баркоды | Critical | Подтверждено трассировкой |
| C-13 | Fallback-расчёт цены расходится с фактически списанным | Medium | Подтверждено трассировкой |
| C-14 | `TotalCost` в событии делится на `successCount` повторно | Medium | Подтверждено трассировкой |
| C-15 | `quote.Partial` без `failedCount` публикует `partial_completed` с `ReleasedUnits=0` | Low | Подтверждено трассировкой |
| C-16 | `allowPartial` проверяется дважды с разной семантикой | Medium | Подтверждено трассировкой |
| C-17 | Нет верхней границы `Units` | Medium | Подтверждено трассировкой |
| C-18 | `source=user` в цепочке попадает в `Skipped` без проверки заполненности | Medium | Подтверждено трассировкой |

---

## C-01. N единиц генерируются с идентичными данными, включая номер документа

**Суть.** При `units=3` сервис возвращает три баркода с полностью одинаковым содержимым, включая поля с `source: random` (номер документа). Цепочка вычислений выполняется один раз *до* цикла генерации, и её результат переиспользуется для всех N единиц.

**Место.** `internal/usecase/generate_usecase.go:162` (вызов цепочки) и `:245-246` (цикл генерации).

**Подробное описание.**

Порядок в `Execute`:

```go
// :162 — цепочка выполняется ОДИН раз
if u.chain != nil && len(req.Fields) > 0 {
    chainResult, chainErr := u.chain.Execute(ctx, req.Revision, req.Fields)
    ...
    resolvedFields = chainResult.Fields
}
...
// :245 — цикл по N единицам переиспользует те же resolvedFields
for i := 0; i < generateCount; i++ {
    item, genErr := generateWithRetry(ctx, u.barcode, req, resolvedFields, ...)
```

`resolvedFields` — одна и та же map для всех итераций. Внутри `ChainExecutor.Execute` шаги с `source: "random"` вызывают `barcodeGen.Random(...)` (`chain_executor.go:82`) ровно один раз за весь запрос.

Проба задала ревизию с шагом `{Field: "DAQ", Source: "random"}`, где стаб `Random()` возвращает уникальное значение на каждый вызов:

```
PROBE C-01: units=3 -> barcodes=3, Random() вызван 1 раз(а)
PROBE C-01: DAQ по единицам: [rnd-DAQ-1] [rnd-DAQ-1] [rnd-DAQ-1]
PROBE C-01 ПОДТВЕРЖДЕНО: все 3 единицы имеют ОДИНАКОВЫЙ DAQ (rnd-DAQ-1)
PROBE C-01: событий barcode.generated=3, все с одинаковыми Fields
```

`Random()` вызван один раз на три единицы. Все три баркода несут один номер документа.

Почему это Blocker, а не «дубликаты в выдаче»: `units` — это количество оплаченных единиц, и биллинг списывает за каждую (`Capture(sagaID, successCount)` с `successCount=3`). Пользователь платит за 3 единицы и получает 3 копии одного объекта. Для домена (реплики документов с уникальным номером `DAQ`) три идентичных номера — не просто бесполезный результат, а неверный: номер документа обязан быть уникальным. Кроме того, в History улетают три события `barcode.generated` с идентичным `Fields`, что делает историю неотличимой от дублирующей записи одного баркода.

Отдельно: если у BarcodeGen на стороне сервиса есть дедупликация по содержимому, три идентичных запроса могут вернуть один и тот же артефакт, и пользователь получит один URL трижды.

**Предлагаемый багфикс.** Перенести выполнение цепочки внутрь цикла, чтобы поля с `source: random` перегенерировались на каждую единицу, а `calculate`-поля пересчитывались от новых random-значений:

```go
for i := 0; i < generateCount; i++ {
    unitFields := req.Fields
    if u.chain != nil && len(req.Fields) > 0 {
        chainResult, chainErr := u.chain.Execute(ctx, req.Revision, req.Fields)
        if chainErr != nil {
            failedCount++
            continue
        }
        unitFields = chainResult.Fields
        if i == 0 {
            computed, skipped = chainResult.Computed, chainResult.Skipped
        }
    }
    item, genErr := generateWithRetry(ctx, u.barcode, req, unitFields, buildBarcodeGenIdempotencyKey(req.IdempotencyKey, i))
    ...
}
```

Это увеличивает число обращений к BarcodeGen (`Calculate`/`Random` × N). Если это неприемлемо по нагрузке, альтернатива — расширить порт `BarcodeGenClient` батч-методом `RandomBatch(ctx, revision, field, params, count)` и заполнять N наборов за один вызов.

Требуется решение владельца ТЗ: п.4 не оговаривает семантику цепочки при `units > 1`. Развилка — либо N независимых документов (тогда фикс выше), либо N копий одного (тогда нужно списывать за 1 единицу, а не за N). Текущее поведение — худшее из двух: платим за N, получаем копии.

**Комментарий.** Это единственная находка уровня Blocker в разделе C. Она не проявляется в тестах, потому что все существующие тесты `GenerateUseCase` используют либо `units=1`, либо стабы, возвращающие константу из `Random()`, — при константе баг невидим. Проба специально сделала `Random()` счётчиком.

---

## C-02. `sagaID` детерминирован и коллидирует между независимыми запросами

**Суть.** `sagaID` собирается из `userID + BuildID + BatchID`, но `BuildID` и `BatchID` не обязательны. При их отсутствии два независимых запроса одного пользователя получают один `sagaID`, что ломает сопоставление Block/Capture/Release в Billing.

**Место.** `internal/usecase/generate_usecase.go:176`.

**Подробное описание.**

```go
sagaID := fmt.Sprintf("saga-%s-%s-%s", userID, req.BuildID, req.BatchID)
```

Валидация в начале `Execute` (`:93-99`) требует только `Units > 0` и `Revision != ""`. `BuildID`/`BatchID` не проверяются нигде — ни в usecase, ни в биндинге запроса.

Проба выполнила два последовательных независимых запроса без этих полей:

```
PROBE C-02: sagaIDs=[saga-user-1-- saga-user-1--]
PROBE C-02 ПОДТВЕРЖДЕНО: два независимых запроса получили ОДИН sagaID "saga-user-1--"
```

Последствия в Billing, который использует `sagaID` как идентификатор саги:

- `Block(sagaID, 1)` дважды — второй Block либо перезапишет первый, либо будет отвергнут как дубликат.
- `Capture(sagaID, 1)` от первого запроса может закрыть блокировку, созданную вторым.
- `Release(sagaID, N)` от упавшего запроса разблокирует средства успешного.

Комментарий в коде на `:173-175` заявляет, что «уникальность buildID+batchID в рамках одного пользователя — ответственность клиента». Проблема в том, что ответственность объявлена, но не проверена: клиент может просто не передать поля, и BFF молча соберёт коллидирующий ID. Контракт, который не валидируется, — не контракт.

**Предлагаемый багфикс.** Два уровня.

1. Валидировать обязательность на входе (`:93`), рядом с проверкой `Revision`:

```go
if strings.TrimSpace(req.BuildID) == "" {
    return domain.GenerateResponse{}, domain.NewValidationError("buildId is required")
}
if strings.TrimSpace(req.BatchID) == "" {
    return domain.GenerateResponse{}, domain.NewValidationError("batchId is required")
}
```

2. Не полагаться на клиентские поля для идентификатора саги. Правильнее — генерировать `sagaID` на стороне BFF и логировать его связь с `buildID`/`batchID`:

```go
sagaID := "saga-" + uuid.NewString()
```

Если `sagaID` должен быть воспроизводим для идемпотентных ретраев (см. раздел B), выводить его из `IdempotencyKey`, который для этого и предназначен:

```go
sagaID = "saga-" + hashHex(userID, req.IdempotencyKey)
```

**Комментарий.** Связано с C-03: там же выясняется, что и `IdempotencyKey` не обязателен. То есть у саги сейчас нет ни одного гарантированно уникального идентификатора.

---

## C-03. Ключ идемпотентности BarcodeGen становится пустым при пустом входном ключе

**Суть.** `buildBarcodeGenIdempotencyKey` при пустом базовом ключе возвращает пустую строку, и все N вызовов BarcodeGen уходят вообще без ключа идемпотентности.

**Место.** `internal/usecase/generate_usecase.go:494-499`.

**Подробное описание.**

```go
func buildBarcodeGenIdempotencyKey(base string, index int) string {
    if base == "" {
        return ""
    }
    return fmt.Sprintf("%s:%d", base, index)
}
```

`req.IdempotencyKey` не обязателен — валидация его не требует. Проба C-01 зафиксировала это как побочный результат:

```
PROBE C-01: idempotencyKeys=[  ] (различаются по индексу)
```

Три вызова получили три пустые строки. Вывод пробы про «различаются по индексу» — неверный, и это как раз доказательство бага: ключи не различаются, потому что их нет.

Последствия: при retry внутри `generateWithRetry` (`:454`) повторный вызов BarcodeGen идёт без ключа, поэтому BarcodeGen не может распознать его как повтор. Если первая попытка на самом деле дошла и создала артефакт, но ответ потерялся на таймауте, retry создаст второй артефакт. Пользователь оплатил 1 единицу, BarcodeGen отдал 2 — расход на стороне BarcodeGen не сходится с `Capture`.

**Предлагаемый багфикс.** Никогда не отдавать пустой ключ вниз. Если клиент не передал свой — вывести детерминированный из параметров саги:

```go
func buildBarcodeGenIdempotencyKey(base, sagaID string, index int) string {
    if base == "" {
        base = sagaID // sagaID уникален после фикса C-02
    }
    return fmt.Sprintf("%s:%d", base, index)
}
```

Вызов на `:246` соответственно передаёт `sagaID`. Дополнительно — сделать `IdempotencyKey` обязательным на уровне HTTP-контракта для мутирующих операций (см. раздел B).

**Комментарий.** Это тот же класс проблемы, что C-02: сущность, от которой зависит корректность списаний, объявлена опциональной.

---

## C-04. AI-подпись генерируется без запроса клиента

**Суть.** Условие авто-триггера вызывает платный AI-сервис всегда, когда в полях нет `signatureUrl`, даже если клиент явно не просил генерацию подписи.

**Место.** `internal/usecase/generate_usecase.go:181-185`.

**Подробное описание.**

```go
_, hasSignature := resolvedFields["signatureUrl"]
if req.GenerateSignature || !hasSignature {
```

Второй операнд делает вызов AI поведением по умолчанию: любой запрос, в котором не передан `signatureUrl`, инициирует `u.ai.GenerateSignature(...)`.

Проба отправила запрос с `GenerateSignature: false` и без `signatureUrl`:

```
PROBE C-03: GenerateSignature=false, но AI.GenerateSignature вызван 1 раз(а)
PROBE C-03 ПОДТВЕРЖДЕНО: авто-триггер вызвал AI без запроса клиента
```

Проблемы:

1. Клиент, которому подпись не нужна, всё равно оплачивает её латентностью — вызов синхронный (`:187`), в критическом пути перед `Block`.
2. Флаг `GenerateSignature: false` фактически ничего не отключает. Единственный способ не вызвать AI — передать непустой `signatureUrl`.
3. Проверка `hasSignature` использует `_, ok := map[...]` вместо `hasUserValue`, поэтому `signatureUrl: ""` или `signatureUrl: nil` считаются «заполнено» и AI не вызывается — семантика противоположна остальному коду, который для этого использует `hasUserValue` (`chain_executor.go:104`).

Пункт 3 даёт вторую аномалию: чтобы отключить AI, достаточно передать пустую строку, что выглядит как «поле не заполнено», но трактуется как «заполнено».

**Предлагаемый багфикс.** Развести явный запрос и авто-триггер, и привести проверку к общей семантике:

```go
hasSignature := hasUserValue(resolvedFields, "signatureUrl")
if req.GenerateSignature && !hasSignature {
    // явный запрос: ошибка AI критична
} 
```

Если авто-триггер действительно нужен по ТЗ п.9.3, он должен быть управляемым конфигом, а не безусловным:

```go
if (req.GenerateSignature || (u.autoSignature && !hasSignature)) {
```

где `u.autoSignature` приходит из конфигурации, по умолчанию `false`.

**Комментарий.** Требует сверки с ТЗ п.9.3: комментарий на `:183-184` описывает авто-триггер как намеренный. Даже если он намеренный, использование `_, ok :=` вместо `hasUserValue` — дефект независимо от решения.

---

## C-05. `fullName` собирается как `"<nil> <nil>"`

**Суть.** Имя для AI-подписи склеивается через `%v` из двух полей map без проверки их наличия. При отсутствии полей в AI уходит буквальная строка `"<nil> <nil>"`.

**Место.** `internal/usecase/generate_usecase.go:186`.

**Подробное описание.**

```go
fullName := fmt.Sprintf("%v %v", resolvedFields["firstName"], resolvedFields["lastName"])
```

Индексация отсутствующего ключа в `map[string]any` даёт `nil`, а `%v` от `nil` печатает `<nil>`. Проба отправила запрос без `firstName`/`lastName`; стаб AI вернул полученное имя в URL:

```
PROBE C-03: signatureUrl="sig://<nil> <nil>"
PROBE C-03 ПОДТВЕРЖДЕНО: fullName собран как "<nil> <nil>"
```

В AI-сервис уходит запрос на генерацию подписи для человека с именем `<nil> <nil>`. Результат — сгенерированная подпись с мусорным содержимым, которая затем записывается в `resolvedFields["signatureUrl"]` (`:203`) и попадает в баркод и в событие `barcode.generated`.

Усиливается C-04: поскольку AI вызывается автоматически, этот путь достигается запросом, который вообще не упоминает ни подпись, ни имя.

Частичный случай не лучше: при наличии только `firstName` получается `"John <nil>"`.

**Предлагаемый багфикс.** Собирать имя из проверенных строк и не вызывать AI, если имени нет:

```go
first, _ := resolvedFields["firstName"].(string)
last, _ := resolvedFields["lastName"].(string)
fullName := strings.TrimSpace(first + " " + last)
if fullName == "" {
    if req.GenerateSignature {
        return domain.GenerateResponse{}, domain.NewValidationError(
            "firstName and lastName are required for signature generation")
    }
    // авто-триггер без имени — пропускаем AI
} else {
    // вызов AI
}
```

**Комментарий.** Тот же паттерн `%v` от возможного `nil` стоит поискать по всему коду — в `generateBarcode` (`:476`) поле `data` берётся через безопасный type assert `fields["data"].(string)`, то есть в кодовой базе есть и правильный образец.

---

## C-06. `PARTIAL_FUNDS` — успешное состояние, переданное через канал ошибки с HTTP 200

**Суть.** Ситуация «доступно меньше, чем запрошено, подтвердите» возвращается как `*domain.AppError` с `HTTPStatus: 200`, смешивая успешный ответ с ошибкой.

**Место.** `internal/usecase/generate_usecase.go:124-129`.

**Подробное описание.**

```go
if quote.Partial && !req.Confirmed {
    return domain.GenerateResponse{}, &domain.AppError{
        Code:       domain.ErrCodePartialFunds,
        HTTPStatus: 200,
        Message:    fmt.Sprintf("only %d of %d units available, set confirmed=true to proceed", ...),
    }
}
```

Проба:

```
PROBE C-04 ПОДТВЕРЖДЕНО: возвращён error code=PARTIAL_FUNDS HTTPStatus=200
PROBE C-04: барcodes в ответе=0, Block вызван 0 раз(а) -> генерации НЕ было
PROBE C-04: success-состояние передаётся через error-канал с HTTP 200
```

Почему это дефект, а не стилистика:

1. Любой вызывающий, который делает `if err != nil { return err }` — а это нормальный Go — превратит запрос подтверждения в ошибку. Так и происходит в `bulk_job_handler.go`: там результат `Execute` проверяется на `err != nil` и публикуется как неуспех, то есть в bulk-пути `PARTIAL_FUNDS` станет ошибкой задачи вместо запроса подтверждения (и подтвердить в bulk-контексте некому — это отдельный пробел ТЗ).
2. `HTTPStatus: 200` внутри типа `AppError` заставляет обработчик ошибок отдавать 200, поэтому по HTTP-коду клиент не отличит это состояние от успеха, а по наличию `error` в теле — не отличит от ошибки.
3. Метрика `RequestsTotal` пометит такой запрос как `error` (см. раздел F, F-15), искажая error rate: нормальный флоу подтверждения будет выглядеть как отказ.

**Предлагаемый багфикс.** Вынести это состояние в успешный ответ отдельным полем, а не в ошибку:

```go
if quote.Partial && !req.Confirmed {
    return domain.GenerateResponse{
        Success:            false,
        ConfirmationNeeded: &domain.PartialConfirmation{
            AllowedTotal:  quote.AllowedTotal,
            RequestedUnits: req.Units,
            UnitPrice:     quote.UnitPrice,
        },
    }, nil
}
```

Обработчик отдаёт 200 с этим телом, а `err == nil` сохраняет корректность для всех вызывающих, включая bulk. Требуется добавить поля в `domain.GenerateResponse` и обновить контракт API.

**Комментарий.** Если менять контракт нельзя, минимальная мера — выделить отдельный тип (`domain.ConfirmationRequired`), не являющийся `error`, и вернуть его третьим значением. Но чистое решение — через успешный ответ.

---

## C-07. Retryable-классификация по подстроке в тексте ошибки

**Суть.** Решение о повторе запроса к BarcodeGen принимается поиском подстрок `"status 500"`, `"status 502"` и т. д. в тексте ошибки. Ошибка 4xx, в теле которой встречается такая подстрока, будет ретраиться.

**Место.** `internal/usecase/generate_usecase.go:505-516`.

**Подробное описание.**

```go
msg := err.Error()
return strings.Contains(msg, "status 500") ||
    strings.Contains(msg, "status 502") ||
    strings.Contains(msg, "status 503") ||
    strings.Contains(msg, "status 504") ||
    strings.Contains(strings.ToUpper(msg), "ECONNREFUSED")
```

Текст ошибки формируется в адаптере (`internal/adapters/barcodegen/http_client.go:224`) с включением **тела ответа**:

```go
return fmt.Errorf("barcodegen: %s: status %d: %s", path, resp.StatusCode, string(errBody))
```

То есть тело ответа BarcodeGen попадает в строку, по которой затем классифицируется retryability. Проба:

```
PROBE C-06: настоящая 500 от barcodegen                   -> retryable=true
PROBE C-06: 400 с текстом про status 500 в теле           -> retryable=true
PROBE C-06: валидация с числом 500 в данных               -> retryable=true
PROBE C-06 ПОДТВЕРЖДЕНО: классификация по strings.Contains -> 4xx с подстрокой ретраится
```

Третий кейс особенно показателен: ответ `status 422` с сообщением валидации, содержащим `status 503`, классифицируется как retryable. Невалидный запрос будет отправлен 3 раза с задержками 1s + 3s, добавив 4 секунды латентности к гарантированно провальной операции. При `units=10` это 40 секунд на запрос, который не мог быть выполнен.

Обратная ошибка тоже возможна: если BarcodeGen изменит формат сообщения (например, на `HTTP 500` или `code=500`), настоящие 5xx перестанут ретраиться, и `maxBarcodeGenRetries` станет мёртвой настройкой. Это молчаливая деградация — тесты не заметят.

**Предлагаемый багфикс.** Передавать статус-код типизированно, а не через текст. Ввести типизированную ошибку в адаптере:

```go
// internal/domain/errors.go
type UpstreamError struct {
    Service    string
    StatusCode int
    Body       string
}
func (e *UpstreamError) Error() string {
    return fmt.Sprintf("%s: status %d: %s", e.Service, e.StatusCode, e.Body)
}
```

Адаптер возвращает её вместо `fmt.Errorf`:

```go
return &domain.UpstreamError{Service: "barcodegen", StatusCode: resp.StatusCode, Body: string(errBody)}
```

Классификатор становится однозначным:

```go
func isRetryableBarcodeGenError(err error) bool {
    if err == nil { return false }
    if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) { return false }
    var upstream *domain.UpstreamError
    if errors.As(err, &upstream) {
        return upstream.StatusCode >= 500 || upstream.StatusCode == 429
    }
    var netErr net.Error
    if errors.As(err, &netErr) { return true }
    return errors.Is(err, syscall.ECONNREFUSED)
}
```

Заодно закрывает `429 Too Many Requests`, который сейчас не ретраится вообще, хотя это классический retryable-статус.

**Комментарий.** Связано с D-02: адаптеры не возвращают типизированные ошибки в принципе, поэтому этот фикс — часть общей работы по контракту ошибок между слоями.

---

## C-08. Нет топологической сортировки цепочки — порядок в YAML определяет успех

**Суть.** `ChainExecutor` обходит шаги в порядке объявления и проверяет `dependsOn` против уже вычисленного. Если шаг объявлен раньше своей зависимости, запрос падает с `MISSING_DEPENDENCY`, хотя конфигурация логически корректна.

**Место.** `internal/usecase/chain_executor.go:55-72`.

**Подробное описание.**

Цикл `for _, step := range cfg.CalculationChain` идёт строго по слайсу. Проверка зависимостей (`:66`) смотрит в `resolvedFields`, куда результаты попадают только по мере выполнения (`:93`). Значит, зависимость обязана быть либо во вводе пользователя, либо в *ранее* объявленном шаге.

Проба задала логически валидную конфигурацию, где `DBB` зависит от `DAQ`, но объявлен раньше:

```
PROBE C-05 ПОДТВЕРЖДЕНО: [MISSING_DEPENDENCY] field "DBB" requires: [DAQ]
PROBE C-05: порядок шагов в YAML определяет успех; топологической сортировки нет
```

Последствия: конфигурация ревизии становится хрупкой. Администратор, добавляющий шаг через `PUT /admin/revisions/:name` (`admin_handler.go:99`), обязан вручную соблюсти топологический порядок; при ошибке порядка ревизия перестаёт работать для всех пользователей, а сообщение об ошибке (`MISSING_DEPENDENCY: field "DBB" requires [DAQ]`) указывает на симптом, а не на причину — администратор увидит «не хватает DAQ», хотя `DAQ` в цепочке есть.

Отсутствует также детект настоящих циклов: `A dependsOn B`, `B dependsOn A`. Бесконечного цикла не будет (обход линейный), но пользователь получит `MISSING_DEPENDENCY` без указания на то, что конфигурация принципиально невыполнима. Ошибка конфигурации диагностируется как ошибка данных запроса.

**Предлагаемый багфикс.** Отсортировать цепочку топологически перед выполнением и валидировать граф при сохранении конфигурации.

В `ChainExecutor.Execute` после получения `cfg`:

```go
ordered, cycleErr := topoSortChain(cfg.CalculationChain, userInput)
if cycleErr != nil {
    return domain.ChainResult{}, cycleErr // CHAIN_CONFIG_ERROR 500 — вина конфига, не клиента
}
for _, step := range ordered { ... }
```

где `topoSortChain` — обход Кана: узлы, зависимости которых удовлетворены вводом пользователя или другими шагами, выпускаются первыми; остаток при непустой очереди означает цикл.

Главное — валидировать граф на записи, в `UpdateConfig`, чтобы невыполнимая ревизия не сохранялась:

```go
if err := validateChainGraph(req.CalculationChain); err != nil {
    return domain.NewValidationError("invalid calculationChain: " + err.Error())
}
```

Тогда `MISSING_DEPENDENCY` в рантайме будет означать ровно то, что заявлено: пользователь не передал обязательное входное поле.

**Комментарий.** Разграничение классов ошибок здесь важнее самой сортировки: сейчас ошибка администратора маскируется под ошибку пользователя, и в метриках это будет 4xx вместо алерта на сломанную ревизию.

---

## C-09. Цепочка не выполняется при пустом `Fields`

**Суть.** Условие входа в цепочку требует непустой `req.Fields`. Запрос без полей полностью пропускает вычисления, и в BarcodeGen уходит пустой набор.

**Место.** `internal/usecase/generate_usecase.go:162`.

**Подробное описание.**

```go
if u.chain != nil && len(req.Fields) > 0 {
```

Ревизия, все поля которой вычисляются (`source: random` + `source: calculate` без пользовательского ввода), полностью корректна с точки зрения `RevisionConfig`. Но такой запрос — `Units: 1, Revision: "X", Fields: {}` — не запустит цепочку, потому что `len(req.Fields) == 0`.

Дальше `resolvedFields` остаётся пустой map (`:152-155`), и `generateBarcode` вызывает `GeneratePDF417` с `Fields: {}`. Для `pdf417` ошибки не будет — валидация уйдёт на сторону BarcodeGen, который вернёт ошибку про отсутствующие поля. Но деньги к этому моменту уже заблокированы (`Block` на `:228` выполняется до генерации), так что путь заканчивается `Release` и потраченным впустую циклом retry × 3.

Для `code128` поведение иное: `data` пуст → `NewValidationError` (`:479`) на каждой из N итераций, но эта ошибка не retryable, поэтому цикл завершится быстро — и всё равно после `Block`.

Проверка `len(req.Fields) > 0` выглядит как оптимизация «нечего вычислять, если ввода нет», но она неверна: наличие вычислений определяется конфигурацией ревизии, а не наличием ввода.

**Предлагаемый багфикс.** Убрать зависимость от размера ввода — решение принимает конфигурация:

```go
if u.chain != nil {
    chainResult, chainErr := u.chain.Execute(ctx, req.Revision, resolvedFields)
    ...
}
```

`ChainExecutor` уже корректно обрабатывает пустой `userInput`: копирование пустой map даёт пустую map, а шаги отработают по конфигурации.

**Комментарий.** После фикса C-01 (цепочка внутри цикла) это условие всё равно надо править — иначе баг сохранится в новом месте.

---

## C-10. `validateMinimumSet` проверяет вход до цепочки, поэтому вычисляемое поле нельзя объявить обязательным

**Суть.** Валидация обязательных полей выполняется по «сырому» `req.Fields` до запуска цепочки. Поле, которое цепочка умеет вычислить, при попадании в `RequiredInputFields` сделает ревизию нерабочей.

**Место.** `internal/usecase/generate_usecase.go:104-109` (вызов) и `:521-533` (реализация).

**Подробное описание.**

```go
// :107 — до цепочки (:162)
if appErr := validateMinimumSet(cfg, req.Fields); appErr != nil {
    return domain.GenerateResponse{}, appErr
}
```

`validateMinimumSet` проверяет `cfg.RequiredInputFields` против `input`, которым передан `req.Fields` — до всяких вычислений.

Само по себе это осмысленно: имя `RequiredInputFields` и комментарий «Проверяем только присутствие полей» (`:105`) говорят, что речь именно о вводе. Порядок тоже правильный — валидация до `Quote`, значит деньги не блокируются до проверки (в отличие от C-09).

Дефект — в отсутствии защиты от несогласованной конфигурации. Ничто не мешает администратору указать одно и то же поле и в `RequiredInputFields`, и в `CalculationChain` с `source: calculate`. Тогда:

- Пользователь не передаёт поле, полагаясь на вычисление.
- `validateMinimumSet` падает с `REQUIRED_FIELDS_MISSING`, не дойдя до цепочки.
- Поле, которое сервис умеет вычислить, объявлено обязательным для ввода — цепочка для него недостижима.

Обратная несогласованность тоже не проверяется: поле в `CalculationChain` с `source: "user"` (`chain_executor.go:87`) молча уходит в `Skipped` и не попадает в `RequiredInputFields` автоматически, то есть «обязательное пользовательское поле» может остаться незаполненным без ошибки (см. C-18).

**Предлагаемый багфикс.** Валидировать согласованность конфигурации при сохранении, в `UpdateConfig`:

```go
func validateRevisionConsistency(cfg domain.RevisionConfig) error {
    computed := make(map[string]string, len(cfg.CalculationChain))
    for _, s := range cfg.CalculationChain {
        if s.Source == "calculate" || s.Source == "random" {
            computed[s.Field] = s.Source
        }
    }
    for _, f := range cfg.RequiredInputFields {
        if src, ok := computed[f]; ok {
            return fmt.Errorf("field %q is both requiredInput and computed (source=%s)", f, src)
        }
    }
    return nil
}
```

Дополнительно — синхронизировать `source: "user"` шаги с `RequiredInputFields`, чтобы пользовательские шаги цепочки участвовали в валидации.

**Комментарий.** Как и C-08, это про валидацию конфигурации, а не рантайма. Оба фикса логично сделать одной функцией `validateRevisionConfig`, вызываемой из `UpdateConfig` и при загрузке конфигов на старте.

---

## C-11. Сбой `Release` на partial-пути проглатывается

**Суть.** На пути частичного успеха ошибка компенсирующей транзакции `Release` игнорируется явным `_ =`. Средства за неудавшиеся единицы остаются заблокированными без следа в логах и метриках.

**Место.** `internal/usecase/generate_usecase.go:283-289`.

**Подробное описание.**

```go
if isPartialOutcome {
    // FIXME
    // Capture уже выполнен — клиент получит M баркодов независимо от результата Release.
    // ТЗ не определяет поведение при сбое компенсирующей транзакции Release в случае
    // partial success, поэтому ошибку намеренно игнорируем.
    if failedCount > 0 {
        _ = u.billing.Release(ctx, sagaID, failedCount)
    }
```

Сценарий: `units=10`, сгенерировано 7, упало 3. `Capture(sagaID, 7)` прошёл, `Release(sagaID, 3)` упал. Средства за 3 единицы заблокированы навсегда — сага завершена с точки зрения BFF, никто не вернётся к этой блокировке.

Отличие от пути `successCount == 0` (`:262-270`), где сбой `Release` корректно возвращает `BILLING_ERROR` с указанием `sagaID`: там проблема хотя бы диагностируема. Здесь — нет ни ошибки, ни лога, ни метрики. Единственный след — расхождение баланса, которое обнаружит пользователь.

Комментарий признаёт проблему («FIXME», «ТЗ не определяет»), и решение вернуть клиенту успех обоснованно — баркоды действительно созданы и оплачены. Дефект не в том, что ошибка не возвращается клиенту, а в том, что она **никуда не сообщается**.

**Предлагаемый багфикс.** Сохранить успешный ответ клиенту, но зафиксировать сбой компенсации для операторов:

```go
if failedCount > 0 {
    if relErr := u.billing.Release(ctx, sagaID, failedCount); relErr != nil {
        metrics.CompensationFailuresTotal.WithLabelValues("release_partial").Inc()
        log.Printf("[CRITICAL] release failed after partial success: sagaID=%s units=%d err=%v",
            sagaID, failedCount, relErr)
    }
}
```

С алертом на `CompensationFailuresTotal > 0` (см. раздел F, F-06/F-10) и процедурой ручной сверки по `sagaID`.

Полноценное решение — таблица/стрим незавершённых компенсаций с воркером-ретраером, чтобы `Release` доводился до конца асинхронно. Это уже про architecture, а не про правку строки.

**Комментарий.** Пересекается с A-01 (тот же класс: потерянная компенсация) и F-06 (нет метрик исходов саги). Три раздела указывают на одну дыру — отсутствие наблюдаемости компенсаций.

---

## C-12. Сбой `Capture` после успешной генерации теряет уже созданные баркоды

**Суть.** Если `Capture` падает после успешной генерации, usecase возвращает ошибку и теряет ссылки на созданные баркоды. Средства остаются заблокированными, артефакты созданы, клиент их не получает.

**Место.** `internal/usecase/generate_usecase.go:272-274`.

**Подробное описание.**

```go
if err := u.billing.Capture(ctx, sagaID, successCount); err != nil {
    return domain.GenerateResponse{}, domain.NewBillingError(err)
}
```

К этой строке уже произошло:
- `Block(sagaID, generateCount)` — средства заблокированы.
- N успешных вызовов BarcodeGen — артефакты созданы и лежат в хранилище BarcodeGen.
- `barcodes` содержит их URL.

При сбое `Capture` возвращается пустой `GenerateResponse{}`, и слайс `barcodes` теряется вместе со стеком. Итог:

- Средства заблокированы (не списаны, но и не освобождены — `Release` здесь не вызывается вообще).
- Баркоды существуют, но их URL потеряны навсегда: `PublishBarcodeGenerated` вызывается ниже (`:346`), то есть History о них не узнает.
- Клиент получает 503 и, вероятно, повторит запрос — при пустом `IdempotencyKey` (C-03) это создаст ещё N артефактов и ещё один `Block`.

Асимметрия с обработкой `successCount == 0` (`:262`) заметна: там при сбое `Release` в сообщение ошибки заботливо включён `sagaID` для ручного разбора. Здесь — обычный `NewBillingError(err)` без `sagaID`, поэтому даже вручную найти зависшую блокировку нельзя без разбора логов Billing.

**Предлагаемый багфикс.** Как минимум — не терять контекст и не блокировать ретрай:

```go
if err := u.billing.Capture(ctx, sagaID, successCount); err != nil {
    metrics.CompensationFailuresTotal.WithLabelValues("capture").Inc()
    log.Printf("[CRITICAL] capture failed with %d generated barcodes: sagaID=%s urls=%v err=%v",
        sagaID, successCount, barcodeURLs(barcodes), err)
    return domain.GenerateResponse{}, domain.NewBillingError(
        fmt.Errorf("capture failed (sagaID=%s, %d barcodes generated): %w", sagaID, successCount, err))
}
```

Правильнее — сделать `Capture` ретраебельным (это идемпотентная операция по `sagaID`) и только после исчерпания попыток отдавать ошибку:

```go
if err := captureWithRetry(ctx, u.billing, sagaID, successCount); err != nil { ... }
```

Поскольку `Capture(sagaID, units)` идемпотентен по контракту саги, повтор безопасен, и это устраняет большинство транзиентных сбоев.

**Комментарий.** Оценка Critical, а не Blocker, потому что путь требует сбоя Billing именно в этом окне. Но последствие — «клиент заплатил, артефакты созданы, клиент ничего не получил» — худшее из возможных для платёжного флоу.

---

## C-13. Fallback-расчёт цены расходится с фактически списанным

**Суть.** При `UnitPrice <= 0` стоимость вычисляется как пропорция от суммы по всем источникам, что даёт неверный результат при частичном успехе в сочетании со Split Payment.

**Место.** `internal/usecase/generate_usecase.go:318-327`.

**Подробное описание.**

```go
if quote.UnitPrice > 0 {
    totalCost = float64(successCount) * quote.UnitPrice
} else {
    totalAmount := quote.BySource.Subscription.Amount +
        quote.BySource.Credits.Amount +
        walletAmount
    totalCost = float64(successCount) * totalAmount / float64(max(quote.AllowedTotal, 1))
}
```

Fallback предполагает единую среднюю цену за единицу: `totalAmount / AllowedTotal`. Но Split Payment (п.6/п.7.1) означает разную цену по источникам — подписка может покрывать единицы по нулевой цене, кредиты по одной, кошелёк по другой.

Пример: `AllowedTotal=10`, из них 5 по подписке (`Amount=0`) и 5 из кошелька (`Amount=100`). `totalAmount=100`, средняя цена = 10. При `successCount=5` fallback даёт `totalCost=50`.

Но Billing списывает по waterfall — сначала подписка. Фактически потраченные 5 единиц — это 5 единиц подписки, то есть реальная стоимость 0, а не 50. Расхождение — на всю сумму.

Обратный случай при `successCount=5`, если waterfall выбрал кошелёк: реальная стоимость 100, показано 50.

`totalCost` уходит клиенту в ответе (`:331`), в событие `barcode.generated` (`:349`) и в `trans-history.log` (`:389`). То есть неверная сумма попадает и в UI, и в историю транзакций, и в финансовый лог.

Комментарий на `:312-317` объясняет выбор знаменателя `AllowedTotal` вместо `wallet.Units` — это действительно исправляет более грубую ошибку. Но остаточная проблема — сама идея усреднения при заведомо неоднородных ценах.

**Предлагаемый багфикс.** Не вычислять стоимость в BFF. По п.1.2 ТЗ («BFF не содержит бизнес-логики расчётов») сумму должен возвращать Billing — причём по факту `Capture`, а не по прогнозу `Quote`. Изменить контракт `Capture`:

```go
// ports.BillingClient
Capture(ctx context.Context, sagaID string, units int) (domain.CaptureResult, error)
```

где `CaptureResult` содержит фактические `TotalCost` и `BySource`. Тогда:

```go
captureRes, err := u.billing.Capture(ctx, sagaID, successCount)
...
billing := domain.GenerateBillingResult{
    TotalCost: captureRes.TotalCost,
    BySource:  captureRes.BySource,
}
```

Это устраняет и C-14, и рассинхрон с реальными списаниями. Если менять контракт нельзя сейчас — оставить fallback, но логировать его срабатывание как аномалию (`UnitPrice <= 0` от Billing — сам по себе признак проблемы) и помечать сумму в ответе как оценочную.

**Комментарий.** Требует согласования с владельцем Billing. До изменения контракта любые суммы в ответе при `UnitPrice <= 0` следует считать недостоверными.

---

## C-14. `TotalCost` в событии `barcode.generated` делится на `successCount` повторно

**Суть.** В событие на каждый баркод пишется `billing.TotalCost / successCount`, что корректно только при равномерной цене и даёт накопление ошибки округления.

**Место.** `internal/usecase/generate_usecase.go:349`.

**Подробное описание.**

```go
Billing: &domain.BarcodeGeneratedBilling{
    TotalCost: billing.TotalCost / float64(successCount),
    BySource:  quote.BySource,
},
```

Две проблемы.

Первая — деление float с потерей: `TotalCost=10.00`, `successCount=3` → каждое событие несёт `3.3333333333333335`. Сумма трёх событий не равна `10.00`. History Service, агрегирующий стоимость по событиям, получит расхождение с `trans-history.log`, куда пишется полный `billing.TotalCost` (`:389`). Два источника истины разойдутся на копейки, и сверка не сойдётся.

Вторая — `BySource: quote.BySource` в каждом событии содержит разбивку **всей саги**, не этой единицы. То есть при `successCount=3` каждое из трёх событий заявляет, что израсходовало всю подписку/кредиты/кошелёк саги. Потребитель, суммирующий `BySource` по событиям, получит трёхкратное завышение.

Итого одно событие содержит поле, поделённое на N, и рядом поле, не поделённое вообще. Внутренне противоречивое сообщение.

**Предлагаемый багфикс.** Не раскладывать сумму саги по единицам. Либо публиковать одно агрегированное событие на сагу, либо передавать в событии единицы измерения, а сумму — только на уровне саги:

```go
Billing: &domain.BarcodeGeneratedBilling{
    SagaID:    sagaID,
    SagaTotal: billing.TotalCost,   // сумма саги, одинаковая во всех событиях, помечена как таковая
    Units:     1,
},
```

Если денежное значение на единицу необходимо, использовать целочисленные минимальные единицы (копейки/центы) и распределять остаток детерминированно:

```go
totalCents := int64(math.Round(billing.TotalCost * 100))
base, rem := totalCents/int64(successCount), totalCents%int64(successCount)
// первым rem событиям добавить 1 цент
```

Это гарантирует, что сумма по событиям в точности равна сумме саги.

**Комментарий.** Общая проблема — деньги в `float64` по всему домену (`SourceBreakdown.Amount`, `QuoteResult.UnitPrice`, `TransactionLog.Amount`). Для платёжной системы это отдельный архитектурный долг: переход на целые минимальные единицы стоит рассмотреть до накопления данных.

---

## C-15. `quote.Partial` без сбоев генерации публикует `partial_completed` с `ReleasedUnits=0`

**Суть.** Событие `partial_completed` публикуется и когда генерация полностью успешна, а частичность обусловлена только балансом. В событии `ReleasedUnits=0`, что делает его семантически пустым.

**Место.** `internal/usecase/generate_usecase.go:281-298`.

**Подробное описание.**

```go
isPartialOutcome := failedCount > 0 || quote.Partial
if isPartialOutcome {
    if failedCount > 0 {
        _ = u.billing.Release(ctx, sagaID, failedCount)
    }
    _ = u.events.PublishPartialCompleted(ctx, domain.PartialCompletedEvent{
        SagaID:        sagaID,
        SuccessUnits:  successCount,
        ReleasedUnits: failedCount,   // == 0 при quote.Partial без сбоев
        ...
    })
```

Сценарий: `units=10`, баланс позволяет 7, все 7 сгенерированы успешно. `quote.Partial=true`, `failedCount=0`. Публикуется `partial_completed{SuccessUnits: 7, ReleasedUnits: 0}`, а `saga_completed` — нет (`else` на `:300`).

С точки зрения саги это полный успех: заблокировано 7, сгенерировано 7, списано 7, освобождать нечего. Но потребитель события видит «частичное завершение» и `ReleasedUnits=0` — сообщение, не описывающее никакого отклонения.

Комментарий на `:277-280` объясняет замысел: частичность относится к «пользователь получил меньше, чем запросил». Это осмысленная трактовка, но она смешивает два разных факта в одном событии:

- «Баланса хватило не на всё» — известно на этапе `Quote`, до генерации.
- «Часть генераций упала» — известно после, требует компенсации.

Первое — не про сагу, а про котировку, и уже сообщено клиенту в ответе (`quote.Partial`). Второе — реальное частичное завершение с компенсацией.

Практическое следствие: `saga_completed` не публикуется для успешных саг с partial-котировкой. Потребитель, ждущий `saga_completed` как признак нормального завершения (например, для закрытия задачи в bulk), не получит его и будет считать сагу незавершённой.

**Предлагаемый багфикс.** Определять частичность по факту компенсации, а не по котировке:

```go
if failedCount > 0 {
    if relErr := u.billing.Release(ctx, sagaID, failedCount); relErr != nil { /* см. C-11 */ }
    _ = u.events.PublishPartialCompleted(ctx, domain.PartialCompletedEvent{
        SagaID: sagaID, SuccessUnits: successCount, ReleasedUnits: failedCount, ...
    })
    metrics.PartialSuccessTotal.Inc()
} else {
    _ = u.events.PublishSagaCompleted(ctx, sagaID)
}
```

Информацию «выдано меньше запрошенного» передавать в ответе клиенту (уже есть) и, при необходимости, отдельным полем `RequestedUnits` в `saga_completed`.

**Комментарий.** Требует сверки с ТЗ п.14.4 и с ожиданиями потребителей событий. Severity низкая, но метрика `PartialSuccessTotal` сейчас считает не то, что подразумевает имя, — это исказит любой дашборд по «доле частичных генераций».

---

## C-16. `allowPartial` проверяется дважды с разной семантикой

**Суть.** Флаг `allowPartial` независимо проверяется в `QuoteUseCase` и в `GenerateUseCase`, причём вторая проверка недостижима при штатной конфигурации, а при нештатной — расходится с первой.

**Место.** `internal/usecase/quote_usecase.go:58-70` и `internal/usecase/generate_usecase.go:132-145`.

**Подробное описание.**

В `QuoteUseCase.Execute`:

```go
if !u.allowPartial && result.AllowedTotal < units {
    return domain.QuoteResult{}, &domain.AppError{Code: domain.ErrCodeInsufficientFunds, HTTPStatus: 402, ...}
}
```

В `GenerateUseCase.Execute`, после получения котировки:

```go
if quote.Partial && !u.allowPartial {
    amountRequired := float64(req.Units-quote.AllowedTotal) * quote.UnitPrice
    ...
    return domain.GenerateResponse{}, &domain.AppError{Code: domain.ErrCodeInsufficientFunds, HTTPStatus: 402, ...}
}
```

Оба usecase хранят собственную копию флага, устанавливаемую независимо через `WithPartialSuccessEnabled`. Если `GenerateUseCase` создан с `Quoter`, у которого тот же флаг, вторая проверка мертва: `QuoteUseCase` уже вернул бы 402.

Проблема — в возможности рассинхрона. `GenerateUseCase` принимает интерфейс `Quoter` (`:29`), поэтому в него можно передать любую реализацию, в том числе с `allowPartial=true`, при `GenerateUseCase.allowPartial=false`. Тогда сработает вторая проверка — с другим расчётом `amountRequired`:

- `QuoteUseCase` использует `result.Shortfall.AmountRequired`, если он есть, иначе `(units - allowed) * unitPrice`.
- `GenerateUseCase` — ту же логику, но по `quote.Shortfall`.

Формулы совпадают, но дублирование означает, что при изменении правила расчёта в одном месте второе останется старым. Это классический источник расхождения, а не текущая ошибка.

Дополнительно: конфигурация флага в двух местах допускает состояние, при котором частичный успех разрешён в котировке и запрещён в генерации. Пользователь увидит успешную котировку с `partial: true`, а на генерации получит 402 — противоречивый UX.

**Предлагаемый багфикс.** Оставить решение в одном месте. Поскольку `GenerateUseCase` всегда получает котировку через `Quoter`, проверку логично сосредоточить в `QuoteUseCase` и удалить дубль из `GenerateUseCase`:

```go
// generate_usecase.go — удалить блок :132-145
```

Если политика должна быть настраиваемой на уровне генерации отдельно от котировки, вынести её в общий объект политики, передаваемый обоим:

```go
type BillingPolicy struct { AllowPartial bool }
```

и внедрять одну ссылку в оба usecase, исключив рассинхрон конструктивно.

**Комментарий.** Сейчас это latent-дефект: при текущей сборке в `api_app.go` флаги устанавливаются согласованно. Риск реализуется при рефакторинге или в тестах, где `mockQuoter` (`generate_usecase_test.go:536`) подменяет котировщик — там как раз возможна несогласованная пара.

---

## C-17. Нет верхней границы `Units`

**Суть.** Валидация проверяет только `Units > 0`. Запрос с `units: 1000000` инициирует миллион последовательных вызовов BarcodeGen в рамках одного HTTP-запроса.

**Место.** `internal/usecase/generate_usecase.go:93-95`, цикл на `:245`.

**Подробное описание.**

```go
if req.Units <= 0 {
    return domain.GenerateResponse{}, domain.NewValidationError("units must be greater than zero")
}
```

Верхней границы нет ни здесь, ни в `QuoteUseCase` (`:33`), ни в биндинге HTTP-запроса. Цикл генерации (`:245`) последовательный, без параллелизма и без ограничения по времени, кроме контекста запроса.

Ограничивающий фактор — только баланс: `generateCount = quote.AllowedTotal` при `quote.Partial`. То есть пользователь с большим балансом (или при mock-биллинге, который в dev принимает всё — см. E-06) может запустить сколь угодно долгий цикл.

Последствия:
- Один запрос удерживает горутину и соединение на время N × (латентность BarcodeGen + до 4s retry-задержек при сбоях).
- Слайс `barcodes` растёт неограниченно — при N=10^6 это сотни мегабайт на запрос, и он целиком сериализуется в JSON-ответ.
- Кэш идемпотентности сохранит этот ответ (до 1 МБ на ключ — см. E-11), либо не сохранит и сломает идемпотентность на больших ответах.
- N событий `barcode.generated` публикуются в цикле (`:346`) — залив Kafka из одного HTTP-запроса.

Отсутствие лимита тела запроса (E-11) и rate limiting (E-12) усиливают: это дешёвый вектор исчерпания ресурсов.

**Предлагаемый багфикс.** Ввести конфигурируемый максимум и валидировать на входе:

```go
const defaultMaxUnitsPerRequest = 100

if req.Units > u.maxUnits {
    return domain.GenerateResponse{}, &domain.AppError{
        Code:       domain.ErrCodeValidation,
        HTTPStatus: 400,
        Message:    fmt.Sprintf("units must not exceed %d; use bulk API for larger volumes", u.maxUnits),
    }
}
```

Значение — из конфигурации (`MAX_UNITS_PER_REQUEST`). Для больших объёмов уже есть предназначенный путь — bulk через Kafka (`bulk.tasks`), и синхронный API должен направлять туда, а не пытаться выполнить.

Дополнительно — ограничить время всего цикла отдельным дедлайном, чтобы даже разрешённый максимум не висел бесконечно при медленном BarcodeGen.

**Комментарий.** Пересекается с разделом E (исчерпание ресурсов) и F (нет метрик латентности, поэтому такой запрос не будет виден в мониторинге как аномалия).

---

## C-18. `source=user` в цепочке попадает в `Skipped` без проверки заполненности

**Суть.** Шаг цепочки с `source: "user"` безусловно уходит в `Skipped`, даже если пользователь поле не заполнил. Незаполненное обязательное поле не вызывает ошибки.

**Место.** `internal/usecase/chain_executor.go:86-90`.

**Подробное описание.**

```go
default:
    // source=user: поле должно быть заполнено пользователем, пропускаем
    skipped = append(skipped, step.Field)
    continue
```

Ветка `default` покрывает `source: "user"` и любое неизвестное значение. Проверки, что поле действительно заполнено, здесь нет — она была выше (`:57`), но там результат — тоже `Skipped`:

```go
if hasUserValue(userInput, step.Field) {
    skipped = append(skipped, step.Field)
    continue
}
```

Итог: поле с `source: "user"` попадает в `Skipped` в обоих случаях — и когда заполнено, и когда нет. Различить их по `ChainResult` невозможно.

Два следствия.

Первое: незаполненное пользовательское поле не диагностируется цепочкой. Единственная защита — `validateMinimumSet` по `RequiredInputFields` (`generate_usecase.go:107`), но эти списки не синхронизированы (см. C-10): поле может быть в `CalculationChain` с `source: "user"` и отсутствовать в `RequiredInputFields`. Тогда оно не проверяется нигде, и в BarcodeGen уходит запрос без него.

Второе: `default` молча поглощает опечатки в конфигурации. `source: "calc"` вместо `"calculate"`, `"rand"` вместо `"random"` — поле попадёт в `Skipped` и не будет вычислено, без единого предупреждения. Администратор увидит, что ревизия «работает», но поля пустые.

Семантика `Skipped` в ответе API тоже размывается: по п.4.2 это «поля, пропущенные потому что заполнены пользователем», а фактически туда попадают ещё и невычисленные из-за опечатки, и незаполненные пользовательские.

**Предлагаемый багфикс.** Разделить ветки и не допускать неизвестных значений:

```go
case "user":
    // поле ожидается от пользователя; заполненность проверяет validateMinimumSet
    skipped = append(skipped, step.Field)
    continue
default:
    return domain.ChainResult{}, &domain.AppError{
        Code:       domain.ErrCodeChainConfig,
        HTTPStatus: 500,
        Message:    fmt.Sprintf("unknown chain source %q for field %q", step.Source, step.Field),
    }
```

Опечатка в конфиге станет явной ошибкой 500 (вина конфигурации, не клиента), а не тихим пропуском. Валидацию допустимых значений `source` добавить в `validateRevisionConfig` из C-10, чтобы такая конфигурация вообще не сохранялась.

Для различения причин пропуска — расширить `ChainResult` отдельными списками (`SkippedUserFilled`, `SkippedAwaitingUser`) либо структурой с причиной.

**Комментарий.** Валидация `source` при записи конфигурации закрывает основной риск (опечатки) на уровне, где ошибка ещё дешева. Рантайм-ветка `default` — вторая линия защиты.

---

## Что проверено и дефектом не является

Зафиксировано, чтобы не перепроверять.

**Порядок валидации до блокировки средств.** `validateMinimumSet` (`:107`) и проверки `Units`/`Revision` (`:93-99`) выполняются до `Quote` (`:112`) и до `Block` (`:228`). Невалидный запрос не приводит к блокировке средств. Порядок корректен.

**Сохранение `AppError` из котировки.** На `:114-121` используется `errors.As` для проброса `*domain.AppError` без искажения, и только «сырые» ошибки оборачиваются в `BILLING_ERROR`. Это правильная обработка: `INSUFFICIENT_FUNDS 402` из `QuoteUseCase` доходит до клиента как 402, а не превращается в 503.

**Приоритет пользовательского ввода в цепочке.** Проверка `hasUserValue(userInput, step.Field)` (`chain_executor.go:57`) использует *исходный* `userInput`, а не `resolvedFields`. Это существенно: если бы использовался `resolvedFields`, вычисленное на предыдущем шаге поле выглядело бы как «заполненное пользователем» и последующие шаги ошибочно пропускались. Реализовано верно и соответствует п.4.2 ТЗ.

**Проверка зависимостей против `resolvedFields`.** На `:66` зависимости ищутся в `resolvedFields` (ввод + вычисленное), что верно: зависимость может быть удовлетворена как пользователем, так и предыдущим шагом.

**Семантика `hasUserValue`.** Пустая строка, строка из пробелов и `nil` считаются незаполненными (`:103-112`). Согласуется с п.4.2. Единственное место, где эта семантика нарушена, — проверка `signatureUrl` в C-04.

**Отсутствие бесконечного цикла в цепочке.** Несмотря на отсутствие детекта циклов (C-08), зацикливания не происходит: обход линейный по слайсу, каждый шаг посещается один раз. Циклическая зависимость приводит к `MISSING_DEPENDENCY`, а не к зависанию.

**`max(quote.AllowedTotal, 1)` как защита от деления на ноль.** На `:326` используется встроенный `max` (Go 1.21+), корректно предотвращающий деление на ноль в fallback-расчёте. Отдельно от вопроса корректности самой формулы (C-13) защита реализована.
