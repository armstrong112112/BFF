# Аудит BFF — Раздел B: Идемпотентность и exactly-once по деньгам

Ветка: `codebase-analysis` (HEAD `e3b3be7 Kafka DLQ`)
Дата: 2026-08-01
Область: `X-Idempotency-Key`, `IdempotencyStore`, дедупликация на стороне BarcodeGen и Billing,
`sagaID`, повторная доставка Kafka, DLQ, дедупликация событий у потребителей.
Предыдущий раздел: [`A-concurrency-races.md`](./A-concurrency-races.md).

---

## 0. Методология

**Статический анализ.** Полностью прочитан весь путь ключа идемпотентности от заголовка до
внешних сервисов:

- `internal/transport/http/gin/idempotency_middleware.go` — middleware, `responseCapture`, `markDuplicateResponse`
- `internal/transport/http/gin/router.go` — какие маршруты закрыты идемпотентностью
- `internal/transport/http/gin/api_handler.go` — инжекция ключа в `req.IdempotencyKey` (4 точки)
- `internal/adapters/idempotency/{redis_store,memory_store}.go` — реализации `Get/Reserve/Set/Delete`
- `internal/usecase/generate_usecase.go` — `sagaID`, `buildBarcodeGenIdempotencyKey`, порядок Block/Capture/Release
- `internal/usecase/edit_usecase.go` — `sagaID = "edit-" + barcodeID`, форвардинг ключа
- `internal/transport/kafka/bulk_job_handler.go` + `internal/adapters/kafka/consumer.go` — Kafka-путь, DLQ, коммит offset
- `internal/adapters/{barcodegen,billing}/http_client.go` — что реально уходит в сеть
- `internal/adapters/events/kafka_publisher.go` + `internal/adapters/kafka/producer.go` — форма событий

**Динамическая проверка.** `go test -race` в этом окружении по-прежнему недоступен (нет
cgo-компилятора), но для раздела B он и не нужен: все дефекты детерминированы. Написаны
10 проб (7 на уровне middleware через `httptest`, 3 на уровне usecase/Kafka-хендлера),
все 10 воспроизвели заявленное поведение с первого прогона. Пробы после подтверждения
удалены, в репозиторий не коммитились. Фактический вывод:

```
[PROBE B-01] alice → {"barcodeUrl":"https://cdn/alice","owner":"alice"}
[PROBE B-01] bob   → {"barcodeUrl":"https://cdn/alice","code":"DUPLICATE_REQUEST","owner":"alice"}
             ПОДТВЕРЖДЕНО: bob получил ответ, сгенерированный для alice

[PROBE B-02] 1) confirmed=false → {"code":"PARTIAL_FUNDS","confirmed":false,"message":"only 3 of 10 …"}
[PROBE B-02] 2) confirmed=true  → {"code":"DUPLICATE_REQUEST","confirmed":false,"message":"only 3 of 10 …"}
             хендлер вызван раз: 1 → подтверждение невозможно, вечный реплей

[PROBE B-03] replay status=200 header="true" body={"barcodes":["url1"],"code":"DUPLICATE_REQUEST","success":true}
             ПОДТВЕРЖДЕНО: success=true одновременно с code=DUPLICATE_REQUEST

[PROBE B-04] попытка 1: status=200 size=1048605 | попытка 2: status=200 size=1048605
             хендлер выполнен раз: 2 → крупный ответ не кэшируется, повтор платный

[PROBE B-05] попытки 1..3: status=503, списаний: 3 → идемпотентность не защитила ни один повтор

[PROBE B-06] ключи в BarcodeGen: []string{"", ""}; Block 2× sagaID="saga-user-1-bulk-job-1-batch-1"
             Capture: ["saga-…:1", "saga-…:1"] → одна сага списана дважды

[PROBE B-07] Redis недоступен → status=409 REQUEST_IN_FLIGHT, handlerCalled=false

[PROBE B-08] попытка 1: err=[BILLING_ERROR] capture status 503; попытка 2: err=<nil>
             Block 2× с одним sagaID="saga-user-1-build-1-batch-1", Capture: ["…:2", "…:2"]

[PROBE B-09] ключ длиной 65536 байт принят, status=200, сохранён в store

[PROBE B-10] ключи в BarcodeGen: ["same-key:0", "same-key:0"]
             ПОДТВЕРЖДЕНО: разные данные (ALICE / MALLORY) идут с одинаковым ключом
```

**Карта защищённых маршрутов** (`router.go`) — важно для оценки охвата:

| Маршрут | `idempotencyKeyRequired` | `IdempotencyMiddleware` | Тратит деньги |
|---|---|---|---|
| `POST /api/v1/barcode/generate` | да (52) | да (53) | **да** |
| `POST /api/v1/barcode/generate/pdf417` | да (61) | да (62) | нет (обход биллинга, см. раздел E) |
| `POST /api/v1/barcode/generate/code128` | да (66) | да (67) | нет (обход биллинга) |
| `POST /api/v1/barcode/:id/edit` | да (75) | да (76) | **да** (сжигает право free-edit) |
| `PUT /admin/revisions/:revision` | да (94) | да (95) | нет |
| `PUT /admin/config/topup-bonus` | да (99) | да (100) | нет |
| `PUT /admin/config/timeouts` | да (104) | да (105) | нет |
| `POST /internal/billing/block-batch` | да (118) | да (119) | **да** |
| `bulk.tasks` (Kafka) | — | **нет вообще** | **да** |
| `POST /internal/capabilities/generate-raw` | нет | нет | нет |

Итого: HTTP-путь формально закрыт везде, где нужно; **Kafka-путь не закрыт нигде** — при этом
именно он генерирует объём (bulk-загрузки).

**Ключевая структурная проблема раздела.** Идемпотентность реализована как *кэш HTTP-ответов*
(`ключ → тело ответа`), а не как *журнал операций* (`ключ → состояние саги`). Из этого
следуют почти все дефекты ниже: кэш ничего не знает про пользователя, про тело запроса, про
то, были ли уже списаны деньги, и про то, что «неуспешный ответ» ≠ «ничего не произошло».

---

## 1. Сводка

| ID | Название | Критичность | Класс | Подтверждение |
|---|---|---|---|---|
| B-01 | Ключ идемпотентности не привязан к пользователю → чужой ответ | **Критично** | Изоляция | Проба |
| B-02 | Ключ не привязан к телу запроса → подмена данных под тем же ключом | **Критично** | Целостность | Проба |
| B-03 | `PARTIAL_FUNDS` кэшируется как финальный ответ → `confirmed=true` недостижим | **Критично** | Логика flow | Проба |
| B-04 | Любой не-2xx удаляет маркер, включая ошибки ПОСЛЕ списания | **Критично** | Двойное списание | Проба |
| B-05 | Bulk-путь не передаёт ключ в BarcodeGen → redelivery = двойная оплата | **Критично** | Двойное списание | Проба |
| B-06 | `Set`/`Delete` выполняются на контексте запроса, который уже отменён | **Высокая** | Отмена ctx | Трассировка |
| B-07 | Ответ >1 МБ не кэшируется → повторная платная генерация | **Высокая** | Двойное списание | Проба |
| B-08 | Сбой Redis отдаётся клиенту как `409 REQUEST_IN_FLIGHT` | **Высокая** | Диагностируемость | Проба |
| B-09 | Повтор с тем же `sagaID`: BFF не дедуплицирует Block/Capture | **Высокая** | Двойное списание | Проба |
| B-10 | DLQ хранит батч целиком → реплей оплачивает успешные items заново | **Высокая** | Двойное списание | Трассировка |
| B-11 | События Kafka без `eventId` и без ключа партиции | **Высокая** | Дедуп у потребителей | Трассировка |
| B-12 | Реплей всегда `200` + инъекция `code` в успешное тело | Средняя | Контракт API | Проба |
| B-13 | Ключ не валидируется: длина, charset, префиксная коллизия, общее пространство | Средняя | Безопасность | Проба |
| B-14 | `ENABLE_IDEMPOTENCY=false` тихо снимает защиту от двойных списаний | Средняя | Конфигурация | Трассировка |
| B-15 | Admin PUT: реплей маскирует потерю изменений конфига | Низкая | Логика | Трассировка |

---

## B-01. Ключ идемпотентности не привязан к пользователю

**Суть.** Ключ кэша — это ровно то, что прислал клиент: `X-Idempotency-Key` без префикса
пользователя, без tenant, без маршрута. Любой аутентифицированный пользователь, угадавший
или подсмотревший ключ другого, получает **его** ответ — со ссылками на его баркоды и его
персональными данными в теле.

**Место.**
- `internal/transport/http/gin/idempotency_middleware.go:68` — `key := c.GetHeader("X-Idempotency-Key")`
- там же `:75`, `:85`, `:97`, `:122`, `:124` — все обращения к store идут этим «сырым» ключом
- `internal/adapters/idempotency/redis_store.go:24` — `keyPrefix = "idempotency:"`, единственный namespace
- `internal/transport/http/gin/middleware.go` — `UserJWTMiddleware` кладёт `UserInfo` в контекст **до** этого middleware (`router.go:47` → `:53`), то есть данные для скоупинга уже доступны и просто не используются

**Подробное описание (трассировка).**

1. `router.go:47` — на группу `/api/v1` навешивается `UserJWTMiddleware`, он валидирует JWT и
   сохраняет `UserInfo` в `gin.Context` (доступно через `GetUserInfo(c)`).
2. `router.go:52-53` — на `POST /barcode/generate` дополнительно навешиваются
   `idempotencyKeyRequired` и `IdempotencyMiddleware`. Порядок правильный: пользователь уже
   известен.
3. `idempotency_middleware.go:68` берёт заголовок и **сразу** идёт в `store.Get(ctx, key)`
   (`:75`). `GetUserInfo(c)` не вызывается ни разу в этом файле.
4. `redis_store.go:57` → `GET idempotency:{key}`. Единое плоское пространство имён на всё
   приложение: `/api/v1`, `/admin` и `/internal` пишут в него же (`router.go:95,100,105,119`).

Проба зафиксировала результат буквально: alice выполняет генерацию с ключом
`shared-key-123`, затем bob с тем же ключом получает
`{"barcodeUrl":"https://cdn/alice","owner":"alice","code":"DUPLICATE_REQUEST"}`.
В реальном ответе `GenerateResponse` (`domain/barcode.go:33-42`) это URL готовых баркодов
плюс `billing.bySource` — то есть утечка и артефакта, и биллинговой разбивки.

Отдельно: значения ключей полностью предсказуемы у типовых клиентов (`buildId`, UUID из
формы, `batchId`). Также ключ, выданный сервисным вызовом `/internal/billing/block-batch`,
живёт в том же пространстве, что и пользовательские, — коллизия между доверенным и
недоверенным трафиком.

**Предлагаемый багфикс.**

Скоупить ключ по субъекту и маршруту, а не доверять клиенту:

```go
// idempotency_middleware.go
func IdempotencyMiddleware(store ports.IdempotencyStore, enableIdempotency ...bool) gin.HandlerFunc {
    // ...
    raw := c.GetHeader("X-Idempotency-Key")
    if raw == "" { c.Next(); return }

    subject := "anonymous"
    if info, ok := GetUserInfo(c); ok && info.UserID != "" {
        subject = "user:" + info.UserID
    } else if svc := c.GetString(ctxKeyServiceName); svc != "" {
        subject = "svc:" + svc
    }
    // scope: субъект + метод + шаблон маршрута; ключ клиента хэшируем,
    // чтобы обезвредить длину и произвольные байты (см. B-13).
    key := fmt.Sprintf("%s|%s %s|%x",
        subject, c.Request.Method, c.FullPath(), sha256.Sum256([]byte(raw)))
```

Дополнительно: для `/admin` и `/internal` использовать отдельный `keyPrefix`
(`idempotency:admin:`, `idempotency:svc:`), чтобы пространства не пересекались.

**Комментарий.** Дефект не в невнимательности, а в выбранной абстракции: middleware написан
как чистый HTTP-кэш и сознательно не знает про домен. При этом всё необходимое (`UserInfo`
в контексте, правильный порядок middleware) уже есть — исправление локальное, в один файл.
Важно: это одновременно баг безопасности (IDOR через кэш) и баг корректности.

---

## B-02. Ключ не привязан к телу запроса

**Суть.** Один и тот же ключ с **разным** телом трактуется как повтор. Отсюда два разных
следствия: (а) клиент, повторивший запрос с исправленными данными под тем же ключом,
получает старый ответ и молча теряет правку; (б) ключ, отданный в BarcodeGen, не зависит от
полей — сервис-получатель тоже не может отличить «тот же запрос» от «другие данные, тот же
ключ».

**Место.**
- `internal/transport/http/gin/idempotency_middleware.go:68-75` — тело запроса не читается и не хэшируется
- `internal/usecase/generate_usecase.go:246` — `buildBarcodeGenIdempotencyKey(req.IdempotencyKey, i)`
- `internal/usecase/generate_usecase.go:489-494` — `return fmt.Sprintf("%s:%d", base, index)`
- `internal/usecase/edit_usecase.go:130,143` — `IdempotencyKey: req.IdempotencyKey` (даже без индекса)

**Подробное описание (трассировка).**

Ключ для BarcodeGen формируется как `<клиентский ключ>:<индекс>` и больше ни от чего не
зависит — ни от ревизии, ни от полей, ни от `barcodeType`. Проба B-10: два вызова
`GenerateUseCase.Execute` с одинаковым `IdempotencyKey: "same-key"`, но с
`firstName: ALICE` и `firstName: MALLORY`, дали в BarcodeGen **один и тот же** ключ
`"same-key:0"`. Если BarcodeGen честно реализует идемпотентность (а ключ ему передают именно
для этого, см. комментарий `barcodegen/http_client.go:141-144`), то второй запрос вернёт
баркод, сгенерированный по данным ALICE, — при том что BFF уже списал деньги за MALLORY и
опубликовал `barcode.generated` с полями MALLORY. Итог: артефакт не соответствует ни истории,
ни оплате.

В edit-пути хуже: `edit_usecase.go:130` передаёт клиентский ключ **как есть**, без индекса и
без учёта `field`/`value`. Правка `field=city` и правка `field=eyeColor` под одним ключом
неразличимы для BarcodeGen.

На уровне middleware то же самое: `capture` не сравнивает тело нового запроса с телом,
породившим кэш. HTTP-семантика идемпотентности (RFC 9110 / Stripe-подобная) требует в этом
случае отвечать `422 Unprocessable Entity` («ключ переиспользован с другим payload»), а не
отдавать чужой результат.

**Предлагаемый багфикс.**

1. В middleware считать отпечаток запроса и хранить его рядом с ответом:

```go
type record struct {
    RequestHash string `json:"requestHash"`
    Status      int    `json:"status"`
    Body        []byte `json:"body"`
}
// при Get: если rec.RequestHash != hash(текущее тело) → 422 IDEMPOTENCY_KEY_REUSED
```

Тело придётся буферизовать (`io.ReadAll` + `c.Request.Body = io.NopCloser(bytes.NewReader(buf))`)
с жёстким лимитом (например, 1 МБ, симметрично `maxIdempotencyBodySize`).

2. Ключ для BarcodeGen сделать производным от полезной нагрузки:

```go
func buildBarcodeGenIdempotencyKey(base string, index int, revision, barcodeType string, fields map[string]any) string {
    if base == "" { return "" }
    canonical, _ := json.Marshal(canonicalize(fields)) // отсортированные ключи
    sum := sha256.Sum256(append([]byte(revision+"|"+barcodeType+"|"), canonical...))
    return fmt.Sprintf("%s:%d:%x", base, index, sum[:8])
}
```

3. В edit-пути передавать `req.IdempotencyKey + ":" + req.Field` минимум, лучше — тот же хэш.

**Комментарий.** Пункт (2) обязателен и в том случае, если пункт (1) сочтут слишком дорогим:
без отпечатка в ключе форвардинг `X-Idempotency-Key` в BarcodeGen не защищает, а создаёт
новый класс ошибок — «дедупликация не тех запросов». Обратите внимание, что дефект
проявляется только при корректном BarcodeGen: чем лучше сделан сосед, тем хуже последствия.

---

## B-03. `PARTIAL_FUNDS` кэшируется как финальный ответ → `confirmed=true` недостижим

**Суть.** Ответ `PARTIAL_FUNDS` отдаётся со статусом `200` и непустым телом, поэтому
middleware сохраняет его в кэш как «успешный финальный ответ». Клиент, который делает
именно то, что просит `message` («set confirmed=true to proceed»), получает в ответ тот же
`PARTIAL_FUNDS` — навсегда, до истечения 24-часового TTL. Штатный сценарий частичной оплаты
из ТЗ нерабочий.

**Место.**
- `internal/transport/http/gin/api_handler.go:80-91` — `PARTIAL_FUNDS` отдаётся как `c.JSON(http.StatusOK, …)`
- `internal/usecase/generate_usecase.go:141-147` — источник: `AppError{Code: ErrCodePartialFunds, HTTPStatus: 200}`
- `internal/transport/http/gin/idempotency_middleware.go:121-122` — условие кэширования `Status() >= 200 && < 300 && body.Len() > 0`
- `internal/transport/http/gin/router.go:52` — `idempotencyKeyRequired`: обойти проблему «просто не присылать ключ» нельзя, ключ обязателен

**Подробное описание (трассировка).**

1. Клиент: `POST /barcode/generate {units:10, confirmed:false}`, `X-Idempotency-Key: K`.
2. `GenerateUseCase.Execute` → `quote.Partial == true`, `req.Confirmed == false` →
   `generate_usecase.go:141` возвращает `AppError{PARTIAL_FUNDS, HTTPStatus: 200}`.
3. `api_handler.go:82-90` распознаёт код и отдаёт `200` с телом
   `{"code":"PARTIAL_FUNDS","partial":true,"confirmed":false,"message":"only 3 of 10 units available, set confirmed=true to proceed"}`.
4. Middleware (`:121`) видит `200` + непустое тело → `store.Set(ctx, K, body)` с полным TTL
   24 ч (`redis_store.go:90`, `s.ttl` из `config.Idempotency.TTL`).
5. Клиент выполняет инструкцию: тот же запрос с `confirmed:true`. Ключ обычно тот же — это
   логическое продолжение той же операции, и именно так ведут себя SDK, которые генерируют
   ключ один раз на «попытку покупки».
6. `:75` → `Get` → найдено → `:79` `c.Data(200, markDuplicateResponse(cached))`. Хендлер **не
   вызывается**. Клиент снова получает `PARTIAL_FUNDS`, но уже с подменённым `code` →
   `DUPLICATE_REQUEST` (см. B-12), то есть теряется даже возможность понять, что происходит.

Проба B-02 показала точный вывод: `хендлер вызван раз: 1`, второй ответ —
`{"code":"DUPLICATE_REQUEST","confirmed":false,"message":"only 3 of 10 units available, set confirmed=true to proceed","partial":true}`.

Обходной путь для клиента существует (сменить ключ), но он нигде не задокументирован и
противоречит смыслу ключа: «повтор той же операции» и «подтверждение той же операции» — с
точки зрения клиента одно и то же.

**Предлагаемый багфикс.**

Кэшировать только *терминальные* ответы. `PARTIAL_FUNDS` — промежуточный (требует действия
клиента), поэтому его надо исключить явно:

```go
// idempotency_middleware.go, фаза 2b
if isTerminal(capture) {
    _ = store.Set(bgCtx, key, capture.body.Bytes())
} else {
    _ = store.Delete(bgCtx, key)
}

// terminal = 2xx и НЕ требующий подтверждения
func isTerminal(r *responseCapture) bool {
    if r.Status() < 200 || r.Status() >= 300 || r.body.Len() == 0 || r.overflowed {
        return false
    }
    var probe struct{ Code string `json:"code"` }
    if json.Unmarshal(r.body.Bytes(), &probe) == nil {
        switch probe.Code {
        case domain.ErrCodePartialFunds: // ожидается повтор с confirmed=true
            return false
        }
    }
    return true
}
```

Более чистая альтернатива (рекомендуется в паре): убрать «ошибку со статусом 200» из домена —
не протаскивать `PARTIAL_FUNDS` через `error`, а возвращать из usecase явный результат
`QuoteConfirmationRequired`, чтобы транспорт не занимался разбором `AppError` с
`HTTPStatus: 200`. Это же убирает связанную путаницу в `api_handler.go:78-91`.

**Комментарий.** Дефект родился на стыке двух корректных по отдельности решений: ТЗ требует
отдавать `PARTIAL_FUNDS` со статусом 200, а middleware по спецификации кэширует «успешные»
ответы. Никто не проверил их совместно. Это самый «дешёвый в исправлении и дорогой в
проявлении» пункт раздела: он ломает не редкий крайний случай, а основной сценарий
монетизации при недостатке средств.

---

## B-04. Любой не-2xx удаляет маркер, включая ошибки ПОСЛЕ списания

**Суть.** В фазе 2b middleware трактует «ответ не 2xx» как «операция не выполнялась» и
удаляет запись идемпотентности, разрешая немедленный повтор. Но в `GenerateUseCase` есть
ветки, где к моменту ошибки деньги **уже** заблокированы или списаны. Каждый повтор —
новое списание.

**Место.**
- `internal/transport/http/gin/idempotency_middleware.go:121-125` — `else { _ = store.Delete(...) }`
- `internal/usecase/generate_usecase.go:272-274` — `Capture` упал → `503 BILLING_ERROR` **после** успешного `Block` и успешной генерации
- `internal/usecase/generate_usecase.go:256-262` — `Release` упал → `503` при заблокированных средствах
- `internal/usecase/edit_usecase.go:155-167` — `PublishBarcodeEdited` упал → `503` **после** `Block` и перегенерации

**Подробное описание (трассировка).**

Ветка `Capture`-fail (самая опасная, потому что Billing к этому моменту уже мог списать):

1. `Block(units=2)` — успех (`generate_usecase.go:230`), 2 юнита заблокированы.
2. Оба баркода сгенерированы успешно.
3. `Capture(sagaID, 2)` — `generate_usecase.go:272` — таймаут/`503` на стороне Billing.
   Важно: таймаут ≠ «не списано». Billing мог обработать запрос и не успеть ответить.
4. Usecase возвращает `BILLING_ERROR`, `RespondError` отдаёт `503`.
5. Middleware `:124` → `store.Delete(key)` — запись стёрта.
6. Клиент (или его SDK с авторетраем на 5xx) повторяет с тем же ключом. Проверка `Get`
   ничего не находит, `Reserve` проходит, хендлер выполняется **заново**: новый `Block` на те
   же 2 юнита с тем же `sagaID`, новая генерация, новый `Capture`.

Проба B-05 (уровень middleware): три попытки → `списаний: 3`.
Проба B-08 (уровень usecase, реальные вызовы Billing): после сбоя `Capture` повтор дал
`Block` дважды с идентичным `sagaID = "saga-user-1-build-1-batch-1"` и `Capture` дважды на
2 юнита. То есть даже детерминированный `sagaID` (см. B-09) сам по себе не спасает — BFF
шлёт повторные команды, а защита целиком делегирована Billing без какого-либо контракта на
этот счёт.

Замечу, что мотив `Delete` в комментарии `:118-120` правильный (иначе клиент до истечения TTL
будет получать `409` и не сможет ретраить). Ошибка в том, что «разрешить ретрай» и «забыть,
что деньги тронуты» реализованы одним действием.

**Предлагаемый багфикс.**

Хранить не «тело ответа», а состояние операции, и различать *безопасные* и *небезопасные*
для ретрая ошибки:

```go
type opState struct {
    Phase  string `json:"phase"`  // "reserved" | "charged" | "done" | "failed_safe"
    SagaID string `json:"sagaId"`
    Status int    `json:"status"`
    Body   []byte `json:"body"`
}
```

- `failed_safe` (валидация 400, `INSUFFICIENT_FUNDS` 402 до `Block`) → `Delete`, ретрай свободен.
- любая ошибка после `Block` → **не** удалять; сохранить `Phase: "charged"` c `SagaID` и вернуть
  клиенту `503` с `Retry-After`. Повтор с тем же ключом должен не выполнять работу заново, а
  до-завершить сагу (проверить/повторить `Capture`, либо `Release`).

Минимальный вариант, если полный state-machine внедрять дорого: пробросить из usecase флаг
«деньги тронуты» (например, через `AppError.Details["moneyTouched"] = true` или отдельный
код `BILLING_UNCERTAIN`) и в middleware удалять маркер только при его отсутствии.

**Комментарий.** Это ядро раздела: именно здесь «кэш ответов» окончательно расходится с
«журналом операций». Пункт связан с A-01 (компенсация на отменённом ctx): вместе они дают
худший сценарий — деньги заблокированы, компенсация не выполнена, а клиенту при этом активно
предлагается ретраить.

---

## B-05. Bulk-путь не передаёт ключ идемпотентности в BarcodeGen

**Суть.** `BulkJobHandler` формирует `domain.GenerateRequest` без `IdempotencyKey`. Поле
пустое → `buildBarcodeGenIdempotencyKey` возвращает `""` → заголовок `X-Idempotency-Key` в
BarcodeGen не отправляется вовсе. При этом Kafka-консьюмер работает в режиме at-least-once и
явно допускает повторную обработку. Каждая повторная доставка = новые баркоды и повторное
списание.

**Место.**
- `internal/transport/kafka/bulk_job_handler.go:34-42` — сборка `GenerateRequest`, поля `IdempotencyKey` нет
- `internal/usecase/generate_usecase.go:489-494` — `if base == "" { return "" }`
- `internal/adapters/barcodegen/http_client.go:145-148` — `if req.IdempotencyKey != "" { headers[...] }` → заголовок не ставится
- `internal/adapters/kafka/consumer.go:73-81` — обработка и коммит offset, ретраев основного потока нет
- `internal/domain/bulk_job.go` — в контракте сообщения `bulk.tasks` нет поля для ключа идемпотентности

**Подробное описание (трассировка).**

Проба B-06 воспроизвела полный цикл: два вызова `Handle` с одним и тем же
`domain.BulkJobMessage` дали:

```
ключи идемпотентности, отданные в BarcodeGen: []string{"", ""}
вызовов BarcodeGen: 2
Block: 2 раз, sagaID="saga-user-1-bulk-job-1-batch-1" / "saga-user-1-bulk-job-1-batch-1"
Capture: ["saga-user-1-bulk-job-1-batch-1:1", "saga-user-1-bulk-job-1-batch-1:1"]
```

Когда именно случается повторная доставка (все три сценария реальны):

1. `consumer.go:79` — `CommitMessages` вернул ошибку (ребаланс, обрыв соединения с
   брокером). Ошибка только логируется, offset не сдвинут → сообщение будет прочитано снова.
2. SIGTERM между обработкой и коммитом — разобрано в A-04.
3. Ребаланс группы: партиция ушла другому консьюмеру до коммита. Дополнительно риск повышает
   то, что API-процесс поднимает второй reader с тем же `group.id` (A-10).

Существенно, что `sagaID` при этом **детерминирован** (`saga-{user}-bulk-{jobID}-{batchID}`) —
то есть у Billing есть техническая возможность отсечь дубль, но ни требования, ни проверки на
это нет. А вот у BarcodeGen такой возможности нет вообще: ключ пустой.

**Предлагаемый багфикс.**

Вывести ключ идемпотентности детерминированно из идентификаторов сообщения — ничего нового
хранить не нужно:

```go
// bulk_job_handler.go
req := domain.GenerateRequest{
    // ...
    BuildID: fmt.Sprintf("bulk-%s", item.JobID),
    BatchID: msg.BatchID,
    Fields:  item.Fields,
    // ключ стабилен между повторными доставками одного и того же item
    IdempotencyKey: fmt.Sprintf("bulk:%s:%s:%d", msg.BatchID, item.JobID, item.RowNumber),
}
```

Дополнительно:
- если Bulk Service присылает свой ключ — добавить его в `domain.BulkJobMessage`/`BulkJobItem`
  и предпочитать его сгенерированному;
- в `Consumer.Start` не коммитить offset при ошибке публикации в DLQ (см. B-10);
- зафиксировать в контракте с Billing, что повторный `Block` с существующим `sagaID` —
  идемпотентная операция (иначе двойное списание сохранится и после этого фикса).

**Комментарий.** Здесь идемпотентность просто не была протянута во второй транспорт: для HTTP
её сделали тщательно (обязательный заголовок, middleware, форвардинг), а для Kafka —
пропустили целиком, хотя именно bulk даёт основной объём генераций. Фикс дешёвый и
локальный, риск — высокий.

---

## B-06. `Set`/`Delete` выполняются на контексте запроса, который уже отменён

> **↔ Частичное пересечение (не дубль): [G-07](G-dlq-replay-redis.md), [A-01](A-concurrency-races.md).** Общий паттерн «очистка на отменённом `ctx`», но разные call sites и разные фиксы: здесь — `Set`/`Delete` idempotency-store в middleware; G-07 — `Unlock` edit-locker; A-01 — `Release` компенсации саги. Считаются тремя независимыми дефектами.

**Суть.** Обе финализирующие операции идемпотентности используют
`c.Request.Context()` **после** `c.Next()`. Если клиент отключился (а именно тогда повтор и
неизбежен), контекст уже отменён: Redis-команда не выполняется, ответ не кэшируется, маркер
не снимается. Запись зависает как `\x00` in-flight до `shortTTL` = 2 мин: 2 минуты клиент
получает `409`, а после — полностью повторное выполнение платной операции.

**Место.**
- `internal/transport/http/gin/idempotency_middleware.go:122` — `store.Set(c.Request.Context(), …)`
- `internal/transport/http/gin/idempotency_middleware.go:124` — `store.Delete(c.Request.Context(), …)`
- `internal/adapters/idempotency/redis_store.go:89-96` — `client.Set(ctx, …)` уважает отмену
- `internal/adapters/idempotency/redis_store.go:20` — `shortTTL = 2 * time.Minute`

**Подробное описание (трассировка).**

1. Клиент отправляет `POST /barcode/generate`, ключ `K`. `Reserve(K)` → `OK`, маркер `\x00`
   с TTL 2 мин.
2. Хендлер работает долго (по расчёту из раздела A — до сотен секунд при деградации
   BarcodeGen). Клиент/прокси разрывает соединение по своему таймауту → `c.Request.Context()`
   отменён.
3. Работа тем не менее доходит до конца (Go не прерывает горутину; отмена влияет только на
   вызовы, которые её проверяют) — либо доходит до ветки ошибки.
4. `:122` `Set` на отменённом контексте → `redis: context canceled`, результат **не
   сохранён**. Ошибка проглочена (`_ =`).
   Или `:124` `Delete` на отменённом контексте → маркер **не снят**.
5. Далее: маркер жив, ответа нет. Все повторы в течение 2 мин → `409 REQUEST_IN_FLIGHT`
   (ветка `:97-108`, `Get` не находит тела). После истечения TTL — маркер исчезает, и повтор
   выполняет всю работу заново, включая `Block`/`Capture`.

Обратите внимание на комбинацию с B-04: при ошибке хендлера предполагаемое поведение
(«разрешить немедленный ретрай») тоже не срабатывает — `Delete` не выполнился. То есть
поведение системы при отвале клиента не соответствует ни одному из двух задуманных путей.

**Предлагаемый багфикс.**

Финализацию делать на контексте, не связанном с отменой запроса (тот же приём, что
предложен в A-01):

```go
// фаза 2b
bgCtx, cancel := context.WithTimeout(context.WithoutCancel(c.Request.Context()), 5*time.Second)
defer cancel()
if isTerminal(capture) {
    if err := store.Set(bgCtx, key, capture.body.Bytes()); err != nil {
        metrics.IdempotencyStoreErrors.WithLabelValues("set").Inc()
        log.Printf("idempotency: set failed key=%s err=%v", key, err)
    }
} else {
    if err := store.Delete(bgCtx, key); err != nil {
        metrics.IdempotencyStoreErrors.WithLabelValues("delete").Inc()
    }
}
```

И перестать глушить ошибки store: без метрики этот отказ полностью невидим.

**Комментарий.** Тот же корневой дефект, что в A-01 (финализация унаследовала контекст
запроса), но в другом слое, поэтому вынесен отдельным пунктом: чинить надо оба места, и
фикс A-01 сам по себе этот случай не закрывает.

---

## B-07. Ответ >1 МБ не кэшируется → повторная платная генерация

**Суть.** Защита от OOM (`maxIdempotencyBodySize = 1 МБ`) при превышении лимита не просто
отказывается кэшировать — она отправляет запрос по ветке `else` и **удаляет** маркер. Успешно
выполненная и оплаченная операция становится полностью незащищённой от повтора.

**Место.**
- `internal/transport/http/gin/idempotency_middleware.go:20` — `const maxIdempotencyBodySize = 1 << 20`
- `internal/transport/http/gin/idempotency_middleware.go:30-40` — `overflowed = true`
- `internal/transport/http/gin/idempotency_middleware.go:121-125` — `!capture.overflowed` в условии → иначе `Delete`

**Подробное описание (трассировка).**

Проба B-04: хендлер отдаёт 1 МБ+ полезной нагрузки; два запроса с одним ключом →
`хендлер выполнен раз: 2`, ни один ответ не помечен `X-Idempotency-Replayed`.

Когда лимит достижим в этом API:
- `GenerateResponse` содержит `Barcodes []BarcodeItem` (`domain/barcode.go:33-42`). При
  типичной длине URL ~120 байт и накладных расходах JSON ~40 байт лимит достигается около
  6–7 тысяч единиц в одном запросе. `units` сверху не ограничен ничем: валидация только
  `binding:"required,min=1"` (`domain/barcode.go:11`), в usecase — только `req.Units <= 0`
  (`generate_usecase.go:100`).
- Ответ также включает `billing.bySource`, а `barcode.generated` — весь `resolvedFields`;
  при росте схемы ревизии размер ответа растёт.

Дополнительный нюанс: `responseCapture.Write` (`:32`) сравнивает `body.Len() + len(b)` с
лимитом и при превышении **перестаёт накапливать вообще**, ставя `overflowed`. Это корректно
для памяти, но означает, что решение о судьбе записи принимается по факту размера — то есть
поведение идемпотентности зависит от `units`, что для клиента совершенно неочевидно.

**Предлагаемый багфикс.**

При переполнении сохранять не тело, а *факт выполнения*:

```go
if capture.overflowed {
    // тело не кэшируем, но операцию фиксируем: повтор не должен выполняться заново
    _ = store.Set(bgCtx, key, mustJSON(opState{
        Phase:  "done_nobody",
        Status: capture.Status(),
    }))
} else if isTerminal(capture) { /* ... */ }
```

Повтор по такой записи должен возвращать `409 DUPLICATE_REQUEST` (или `303 See Other` на
ресурс результата) — но ни в коем случае не выполнять генерацию снова.

Параллельно ввести верхнюю границу `units` (например, `max=500` в `binding` + проверка в
usecase) и переводить крупные заказы в bulk-путь: это одновременно закрывает и переполнение,
и риск многочасового HTTP-запроса из расчёта в разделе A.

**Комментарий.** Классический случай, когда фикс одной проблемы (OOM, отмечен в коде как
«Техдолг п.5») открыл другую: ветка `else` изначально писалась под ошибки хендлера, и в неё
задним числом добавили ещё одно, семантически противоположное условие.

---

## B-08. Сбой Redis отдаётся клиенту как `409 REQUEST_IN_FLIGHT`

**Суть.** Ошибка `store.Reserve` (недоступен Redis, таймаут, аутентификация) обрабатывается
той же ветвью, что и «параллельный запрос с этим ключом уже выполняется»: клиент получает
`409 REQUEST_IN_FLIGHT`. Ошибка `store.Get` вообще игнорируется. При недоступности Redis API
превращается в 100 % `409` — фактически полный отказ, замаскированный под штатный конфликт.

**Место.**
- `internal/transport/http/gin/idempotency_middleware.go:75-76` — `if err == nil && found` (ошибка `Get` молча отбрасывается)
- `internal/transport/http/gin/idempotency_middleware.go:85-92` — `if err != nil { 409 REQUEST_IN_FLIGHT }`
- `internal/transport/http/gin/idempotency_middleware.go:96-108` — ветка `!reserved`, тот же код ответа

**Подробное описание (трассировка).**

Проба B-07 (store, возвращающий `connection refused` на `Get` и `Reserve`):
`status=409 body={"code":"REQUEST_IN_FLIGHT",…} handlerCalled=false`.

Последствия:
1. Мониторинг видит рост `409`, а не `5xx`. Большинство алертов и SLO настроены на 5xx →
   инцидент незаметен. Метрики `DuplicateRequestsTotal` при этом не растут (инкремент только
   в ветках реплея), то есть и по бизнес-метрикам сигнала нет.
2. Клиенты по спецификации HTTP не ретраят `409` (это конфликт состояния, а не временный
   сбой) — а здесь ретрай как раз и нужен. Заголовок `Retry-After` не отправляется.
3. Диагностика: логов в этой ветке нет вообще, текст ошибки Redis отбрасывается. Причину
   инцидента по логам BFF установить нельзя.

Само решение «fail-closed» (не пускать запрос дальше, если нельзя гарантировать
идемпотентность) для платных операций **правильное** — неверны только код ответа,
наблюдаемость и молчаливое игнорирование ошибки `Get`.

**Предлагаемый багфикс.**

```go
cached, found, err := store.Get(c.Request.Context(), key)
if err != nil {
    metrics.IdempotencyStoreErrors.WithLabelValues("get").Inc()
    log.Printf("idempotency: get failed key=%s err=%v", key, err)
    c.Header("Retry-After", "1")
    RespondError(c, &domain.AppError{
        Code: "IDEMPOTENCY_UNAVAILABLE", HTTPStatus: 503,
        Message: "idempotency store unavailable, retry later",
    })
    c.Abort()
    return
}
// ... то же для Reserve: 503 + Retry-After, а 409 REQUEST_IN_FLIGHT оставить
// исключительно для случая !reserved (реальный параллельный запрос).
```

Плюс добавить метрику `bff_idempotency_store_errors_total{op}` и включить её в алерты.

**Комментарий.** Отдельно стоит отметить: `MemoryStore` никогда не возвращает ошибок, поэтому
в dev и в тестах эта ветка не исполняется вовсе — дефект структурно невидим до продакшена.
Хороший аргумент за тест с «ломающимся» store в CI (проба для него уже написана).

---

## B-09. Повтор с тем же `sagaID`: BFF не дедуплицирует Block/Capture

**Суть.** `sagaID` детерминирован — это плюс, он даёт Billing возможность отсечь дубль. Но
BFF (а) не проверяет, не выполнялась ли уже сага с этим `sagaID`, (б) спокойно отправляет
повторные `Block`/`Capture`, (в) в edit-пути формирует `sagaID` так, что он совпадает у
разных по смыслу операций. Гарантия exactly-once целиком делегирована соседнему сервису без
зафиксированного контракта.

**Место.**
- `internal/usecase/generate_usecase.go:176` — `sagaID := fmt.Sprintf("saga-%s-%s-%s", userID, req.BuildID, req.BatchID)`
- `internal/usecase/edit_usecase.go:108` — `sagaID := "edit-" + barcodeID`
- `internal/usecase/generate_usecase.go:230,272,288` — `Block`/`Capture`/`Release`, ни одной проверки «сага уже существует»
- `internal/adapters/billing/http_client.go:139-152` — `Block`/`Capture` не передают ни `Idempotency-Key`, ни `attempt`

**Подробное описание (трассировка).**

Три отдельных наблюдения.

1. **Повтор.** Проба B-08: после сбоя `Capture` повторный вызов дал `Block` дважды с
   идентичным `sagaID` и `Capture` дважды на 2 юнита. Если Billing не дедуплицирует по
   `sagaID` — двойное списание; если дедуплицирует — второй `Block` вернёт ошибку/конфликт,
   которую BFF интерпретирует как `BILLING_ERROR 503` (`generate_usecase.go:230-232`) и
   отдаст клиенту как сбой, хотя фактически всё в порядке. Оба исхода плохие, а выбор между
   ними зависит от реализации соседа, которую BFF не контролирует.

2. **Пустые компоненты.** `BuildID` и `BatchID` не обязательны (`domain/barcode.go:12-13`, без
   `binding:"required"`). Клиент, не передавший их, получает `sagaID = "saga-user-1--"` —
   ровно это и видно в логе пробы из раздела A. Все запросы такого пользователя делят один
   `sagaID`: если Billing дедуплицирует по нему, второй заказ будет «схлопнут» с первым.
   Ирония в том, что комментарий `generate_usecase.go:172-175` явно перекладывает
   уникальность `buildID+batchID` на клиента — но валидации, которая бы это требование
   обеспечивала, нет.

3. **Edit.** `sagaID = "edit-" + barcodeID` не содержит ни `userID`, ни `field`, ни времени,
   ни попытки. Все правки одного баркода за всё время жизни — одна сага. С учётом A-02
   (лок истекает в середине операции) две параллельные правки регистрируются под одним
   `sagaID`.

**Предлагаемый багфикс.**

1. Сделать `sagaID` производным от ключа идемпотентности, а не от опциональных полей:

```go
// один клиентский ключ ⇒ одна сага; ключ обязателен на этом маршруте (router.go:52)
sagaID := "saga-" + shortHash(userID+"|"+req.IdempotencyKey)
```

Для bulk — из `batchID:jobID:rowNumber` (см. B-05). Для edit — добавить `userID` и `field`:
`"edit-" + shortHash(userID+"|"+barcodeID+"|"+req.Field+"|"+req.IdempotencyKey)`.

2. Ввести `binding:"required"` на `buildId`/`batchId` либо генерировать их на сервере, чтобы
   пустых компонентов не существовало.

3. Зафиксировать контракт с Billing в явном виде: повторный `Block` с существующим `sagaID`
   возвращает состояние существующей саги (200/409 c телом), а не создаёт новую блокировку;
   `Capture`/`Release` идемпотентны по `(sagaID, units)`. И добавить в клиент перед��чу
   `Idempotency-Key: sagaID` в заголовке — сейчас его нет вовсе
   (`billing/http_client.go:180-200`).

**Комментарий.** Здесь код уже сделал 80 % работы: детерминированный `sagaID` — правильная
основа для exactly-once. Не хватает двух вещей: чтобы он не мог быть вырожденным, и чтобы
существовал письменный контракт с Billing. Без второго BFF не может ни гарантировать, ни
даже проверить exactly-once — он лишь надеется на соседа.

---

## B-10. DLQ хранит батч целиком → реплей оплачивает успешные items заново

**Суть.** Единица обработки — сообщение с массивом `items[]`, а единица DLQ — всё сообщение
целиком (`RawValue: m.Value`). Если упал один item из ста, в DLQ уходит весь батч, а offset
всё равно коммитится. Любой реплей DLQ повторно генерирует и оплачивает 99 успешных items.
Плюс отдельный дефект: offset коммитится, даже если публикация в DLQ провалилась — тогда
сообщение теряется полностью.

**Место.**
- `internal/adapters/kafka/consumer.go:73-81` — `handler` → `publishDLQ` → `CommitMessages` (безусловно)
- `internal/adapters/kafka/consumer.go:88-104` — `publishDLQ`: ошибка публикации только логируется
- `internal/transport/kafka/bulk_job_handler.go:30,61-63,82` — `firstErr`: возвращается первая ошибка, состояние остальных items не сохраняется
- `internal/transport/kafka/bulk_job_handler.go:44-53` — `PublishBulkResult` на каждый item есть, но это событие наружу, а не состояние для реплея

**Подробное описание (трассировка).**

1. Батч из 100 items. Item №7 падает (например, `VALIDATION_ERROR`). Items 1–6 и 8–100
   успешно сгенерированы и **оплачены** (`Capture` выполнен для каждого в
   `GenerateUseCase.Execute`).
2. `Handle` возвращает `firstErr` (`bulk_job_handler.go:82`) — ошибку item №7.
3. `consumer.go:73-76` → `publishDLQ(ctx, m, "handler", err)`: в DLQ уходит **весь**
   `m.Value`, т. е. все 100 items. `ErrorDetail` содержит текст ошибки только по item №7.
4. `consumer.go:79` — offset коммитится независимо от результата шага 3.
5. Оператор разбирает `bulk.tasks.dlq` и переотправляет запись в `bulk.tasks` (штатный смысл
   DLQ). Так как `IdempotencyKey` в bulk-пути отсутствует (B-05), а BFF не хранит per-item
   состояние — 99 успешных items генерируются и оплачиваются повторно.

Дополнительно к шагу 4: если брокер DLQ недоступен (`publishDLQ` залогировал ошибку), offset
всё равно сдвигается — сообщение не в основном топике, не в DLQ, и информации о нём нет
нигде, кроме строки лога. Комментарий `consumer.go:86` («Вызывается перед CommitMessages на
обоих путях ошибок») описывает порядок, но не зависимость: успех DLQ на коммит не влияет.

Ещё одна деталь: ошибки бизнес-валидации (item с неполными полями — `VALIDATION_ERROR`)
попадают в DLQ вместе с инфраструктурными. Повторная доставка такого item не может
завершиться успехом никогда — он будет ходить по кругу, если DLQ переотправляют
автоматически.

**Предлагаемый багфикс.**

1. Не коммитить offset, если DLQ-публикация не удалась:

```go
if err := c.handler(ctx, msg); err != nil {
    if dlqErr := c.publishDLQ(ctx, m, "handler", err); dlqErr != nil {
        // не коммитим: пусть сообщение придёт снова, чем потеряется
        metrics.DLQPublishErrors.Inc()
        continue
    }
}
```

(для этого `publishDLQ` должен возвращать `error`, сейчас он `void`.)

2. В DLQ писать **per-item**, а не батч: `DLQRecord` расширить полями `JobID`, `RowNumber` и
   `RawItem`, а `BulkJobHandler` — публиковать в DLQ только упавшие items (он уже итерирует
   по ним и знает, какие именно).

3. Разделить причины: `reason="validation"` (реплей бесполезен, нужен ответ пользователю)
   и `reason="infrastructure"` (реплей осмысленен). Первые — не в DLQ, а в `bulk.result`
   со `Status: "FAILED"` (что уже делается на `bulk_job_handler.go:53-58`).

4. После фикса B-05 реплей становится безопасным и для смешанных случаев — эти два пункта
   надо делать вместе.

**Комментарий.** DLQ добавили последним коммитом (`e3b3be7 Kafka DLQ`), и он решает задачу
«не застрять на плохом сообщении», но пока не задачу «безопасно переобработать». Гранулярность
DLQ должна совпадать с гранулярностью оплаты — в этом коде это item, а не сообщение.

---

## B-11. События Kafka без `eventId` и без ключа партиции

**Суть.** Ни одно исходящее событие не несёт уникального идентификатора, а
`Producer.Publish` не задаёт `kafka.Message.Key`. Потребители (History, Notifications,
TransHistory, Bulk) физически не могут отличить повторную публикацию от новой и не получают
гарантий порядка внутри сущности. Для `barcode.generated`, публикуемого на каждый баркод,
это означает дубликаты в истории транзакций.

**Место.**
- `internal/adapters/kafka/producer.go:44-51` — `WriteMessages(ctx, kafka.Message{Topic: topic, Value: body})`, `Key` не задан
- `internal/adapters/kafka/producer.go:37` — `Balancer: &kafka.LeastBytes{}` — распределение по нагрузке, порядок по сущности не сохраняется
- `internal/domain/events.go` — в структурах событий нет поля `eventId` (есть только `eventType` у `BulkResultEvent`, `events.go:5`)
- `internal/usecase/generate_usecase.go:347-362` — цикл `PublishBarcodeGenerated` на каждый баркод
- `internal/adapters/events/kafka_publisher.go:57-90` — все `Publish*` передают payload как есть

**Подробное описание (трассировка).**

1. Успешная генерация на `units=5` публикует 5 событий `barcode.generated`
   (`generate_usecase.go:348`). Все пять имеют одинаковый набор полей, кроме `barcodeUrl`, и
   ни одно не имеет идентификатора. Тело содержит `userId`/`buildId`/`batchId`, но при
   повторном исполнении хендлера (B-04, B-05, B-07) значения будут **точно такими же**, а
   `barcodeUrl` — новыми. Потребитель, дедуплицирующий по естественному ключу, обязан
   считать их разными записями.
2. `Key` не задан → `LeastBytes` раскидывает события одного и того же баркода/пользователя по
   разным партициям. Порядок между `barcode.generated` и последующим `barcode.edited` для
   одного баркода не гарантируется. Практическое следствие: `barcode.edited` может быть
   обработан раньше `barcode.generated` — History получит правку записи, которой ещё нет.
   Обработка этой ситуации в контракте не описана.
3. `billing.saga.completed` / `partial_completed` несут только `sagaID` и `timestamp`
   (`kafka_publisher.go:22-34`). При повторной публикации (тот же `sagaID`, другой
   `timestamp`) потребитель не различит «повтор» и «вторая сага с тем же ID» — а с учётом
   B-09 второй вариант вполне возможен.

**Предлагаемый багфикс.**

1. Добавить в `Publisher` обязательный ключ партиционирования и идентификатор события:

```go
// producer.go
func (p *Producer) PublishKeyed(ctx context.Context, topic, key string, payload any) error {
    body, err := json.Marshal(payload)
    if err != nil { return fmt.Errorf("kafka producer: marshal %s: %w", topic, err) }
    return p.writer.WriteMessages(ctx, kafka.Message{
        Topic: topic,
        Key:   []byte(key),   // партиционирование и compaction
        Value: body,
    })
}
```

Ключи: `barcode.generated` → `barcodeID` (или `sagaID`), `barcode.edited` → `barcodeID`,
`billing.saga.*` → `sagaID`, `bulk.result` → `jobID`, `notif-events` → `userID`.

2. Ввести в домене общий конверт события:

```go
type EventEnvelope struct {
    EventID   string `json:"eventId"`   // детерминированный: hash(sagaID|index|type)
    EventType string `json:"eventType"`
    Version   int    `json:"version"`
    Timestamp string `json:"timestamp"`
}
```

`EventID` обязательно **детерминированный** (а не `uuid.New()`), иначе повторная публикация
породит новый ID и дедупликация опять не сработает. Для `barcode.generated` естественный
кандидат — `sagaID + ":" + index`.

3. Зафиксировать в контракте, что потребители дедуплицируют по `eventId`.

**Комментарий.** Это единственный пункт раздела, который нельзя закрыть внутри BFF: он
требует согласования с потребителями. Поэтому его стоит начинать раньше остальных —
добавление полей обратно совместимо (потребители игнорируют неизвестные поля), а вот
включение дедупликации на их стороне требует релиза.

---

## B-12. Реплей всегда `200` и инъекция `code` в успешное тело

**Суть.** При реплее middleware (а) всегда отдаёт статус `200`, не сохраняя оригинальный
(`201`/`202`), и (б) внедряет в тело поле `code: "DUPLICATE_REQUEST"`, ломая схему ответа:
клиент получает документ, где одновременно `success: true` и код ошибки.

**Место.**
- `internal/transport/http/gin/idempotency_middleware.go:79` — `c.Data(http.StatusOK, "application/json", markDuplicateResponse(cached))`
- `internal/transport/http/gin/idempotency_middleware.go:99` — то же во второй ветке
- `internal/transport/http/gin/idempotency_middleware.go:129-141` — `markDuplicateResponse`
- в store не сохраняется ни статус, ни `Content-Type` — только тело (`ports.go`, `IdempotencyStore.Set(ctx, key, body []byte)`)

**Подробное описание (трассировка).**

Проба B-03: реплей вернул
`status=200 header="true" body={"barcodes":["url1"],"code":"DUPLICATE_REQUEST","success":true}`.

Проблемы по частям:
1. `success: true` + `code: DUPLICATE_REQUEST` — противоречивый документ. Клиент,
   проверяющий наличие `code` как признак ошибки (а именно так устроен
   `ErrorResponse` в этом же пакете), обработает успешный ответ как ошибку.
2. `markDuplicateResponse` перезаписывает поле `code`, если оно уже было. На этом ломается
   B-03: исходный `PARTIAL_FUNDS` подменяется на `DUPLICATE_REQUEST`, и клиент теряет
   информацию о том, что нужно подтверждение.
3. Статус не сохраняется: если хендлер вернул `202 Accepted`, реплей отдаст `200`. Сейчас все
   защищённые маршруты отвечают `200`, так что проявляется только пункт 1–2, но это
   отложенная мина.
4. `Content-Type` жёстко `application/json` — для не-JSON тела (файл, `image/png`) реплей
   отдаст неверный тип. `markDuplicateResponse` для такого тела корректно возвращает его
   как есть (`:132`), но заголовок всё равно будет неправильным.

**Предлагаемый багфикс.**

Хранить полный слепок ответа и не трогать тело:

```go
type storedResponse struct {
    Status      int    `json:"status"`
    ContentType string `json:"contentType"`
    Body        []byte `json:"body"`
}
// реплей:
c.Header("X-Idempotency-Replayed", "true")
c.Header("X-Idempotency-Original-Status", strconv.Itoa(rec.Status))
c.Data(rec.Status, rec.ContentType, rec.Body)
```

Признак повтора передавать заголовком (`X-Idempotency-Replayed` уже есть и достаточен), а
`markDuplicateResponse` удалить целиком.

**Комментарий.** Мутация тела ответа выглядит как попытка выполнить требование ТЗ о коде
`DUPLICATE_REQUEST`. Если требование действительно есть, корректный способ — заголовок или
отдельная обёртка, но не перезапись поля внутри доменного документа: именно эта перезапись
маскирует `PARTIAL_FUNDS` в B-03.

---

## B-13. Ключ не валидируется: длина, charset, префиксная коллизия, общее пространство

**Суть.** Значение `X-Idempotency-Key` используется как есть: без ограничения длины, без
проверки допустимых символов, без нормализации. Отсюда управляемый клиентом рост памяти/Redis,
возможность коллизий за счёт разделителя `:` и, в связке с B-01, доступ к чужим записям.

**Место.**
- `internal/transport/http/gin/idempotency_middleware.go:68` — заголовок берётся без валидации
- `internal/transport/http/gin/idempotency_middleware.go:144-155` — `idempotencyKeyRequired` проверяет только `!= ""`
- `internal/adapters/idempotency/redis_store.go:24,57,77` — `keyPrefix + key` без экранирования
- `internal/adapters/idempotency/memory_store.go:21-24` — `entries map[string]entry`, размер ключа не ограничен
- `internal/usecase/generate_usecase.go:489-494` — `base + ":" + index`, `base` не экранируется

**Подробное описание (трассировка).**

Проба B-09: ключ длиной 65 536 байт принят, запрос обработан, запись сохранена в store.
Что из этого следует:

1. **Память.** `MemoryStore` (dev-режим и любой инстанс без `REDIS_URL`, см. A-05) хранит
   ключи в map до истечения TTL; очистка — раз в 5 минут (`memory_store.go:110`). Клиент,
   отправляющий ключи по 64 КБ, наращивает потребление памяти линейно и легально. В Redis то
   же самое, только за счёт памяти брокера; ограничение Redis на длину ключа (512 МБ) здесь
   не помогает.
2. **Коллизия через разделитель.** Ключ для BarcodeGen — `base:index`. Клиент, отправивший
   `X-Idempotency-Key: "abc:1"` при `units=1`, получит для индекса 0 ключ `"abc:1:0"`, а
   клиент с ключом `"abc"` при `units≥2` — `"abc:1"`. Пересечения пространств возможны и
   управляются клиентом. Аналогично `"idempotency:" + key` в Redis: ключ, начинающийся с
   `idempotency:`, даёт `idempotency:idempotency:…`.
3. **Проброс наружу.** Ключ уходит в заголовок HTTP-запроса к BarcodeGen
   (`barcodegen/http_client.go:146`). `http.Header.Set` не проверяет значение на CR/LF в
   момент установки — при некорректном значении запрос упадёт на этапе записи (Go проверяет
   заголовки при `Write`), то есть это не header injection, но это `500` из-за
   пользовательского ввода, который никто не вали��ировал.

**Предлагаемый багфикс.**

```go
const maxIdempotencyKeyLen = 255

var idempotencyKeyRe = regexp.MustCompile(`^[A-Za-z0-9._~-]{8,255}$`)

if !idempotencyKeyRe.MatchString(raw) {
    RespondError(c, domain.NewValidationError(
        "X-Idempotency-Key must be 8..255 chars of [A-Za-z0-9._~-]"))
    c.Abort()
    return
}
```

И хэшировать ключ перед использованием в качестве ключа хранилища (см. фикс B-01) — это
одновременно фиксирует длину, устраняет коллизии с разделителями и убирает пользовательский
ввод из ключей Redis и заголовков исходящих запросов.

**Комментарий.** Сам по себе пункт средней важности, но он усиливает B-01 (предсказуемые и
произвольные ключи в общем пространстве имён) и B-02 (коллизии по разделителю). Все три
закрываются одним изменением — нормализацией ключа на входе.

---

## B-14. `ENABLE_IDEMPOTENCY=false` тихо снимает защиту от двойных списаний

**Суть.** Один флаг одновременно (а) отменяет обязательность заголовка и (б) полностью
отключает дедупликацию. Ни лога, ни метрики, ни предупреждения при старте: снаружи
неотличимо от работающей защиты, пока не начнутся двойные списания.

**Место.**
- `internal/config/config.go:60` + `:189` — `EnvEnableIdempotency`, дефолт `true`
- `internal/transport/http/gin/idempotency_middleware.go:57-73` — `if !enabled { c.Next(); return }`
- `internal/transport/http/gin/idempotency_middleware.go:144-150` — `idempotencyKeyRequired`: `if !enabled { c.Next() }`
- `internal/app/api_app.go` — при сборке роутера значение флага никак не логируется

**Подробное описание (трассировка).**

При `ENABLE_IDEMPOTENCY=false`:
1. `idempotencyKeyRequired` пропускает запросы без ключа.
2. `IdempotencyMiddleware` не обращается к store вовсе.
3. `GenerateUseCase` получает `req.IdempotencyKey == ""` →
   `buildBarcodeGenIdempotencyKey` возвращает `""` (`generate_usecase.go:490-492`) → в
   BarcodeGen ключ не уходит. То есть отключается не только защита в BFF, но и дедупликация у
   соседа.
4. `sagaID` остаётся детерминированным, так что единственная линия обороны — поведение
   Billing при повторном `Block` (см. B-09, контракт не зафиксирован).

Флаг предусмотрен ТЗ (п.15) как средство быстрого отката, и сама возможность нужна. Проблема
в отсутствии сигнала: в `/metrics` нет gauge с состоянием флагов, при старте нет
предупреждения, в ответах нет признака деградации.

**Предлагаемый багфикс.**

1. При старте — явный warning и метрика:

```go
if !cfg.Features.EnableIdempotency {
    log.Printf("WARNING: ENABLE_IDEMPOTENCY=false — двойные списания не предотвращаются")
}
metrics.FeatureFlag.WithLabelValues("idempotency").Set(boolToFloat(cfg.Features.EnableIdempotency))
```

2. Запретить комбинацию «прод-профиль + идемпотентность выключена» (в одном ряду с strict-режимом
   для моков из A-05): при `APP_ENV=production` и `ENABLE_IDEMPOTENCY=false` — падать на старте
   либо требовать явный `I_KNOW_WHAT_IM_DOING=true`.

3. Разделить флаг на два: `ENABLE_IDEMPOTENCY_REQUIRED` (обязательность заголовка) и
   `ENABLE_IDEMPOTENCY_STORE` (дедупликация). Тогда откат возможен без полного отключения
   защиты.

**Комментарий.** Пункт того же класса, что A-05 и fail-open конфигурация из первичного
обзора: система предпочитает молча продолжить работу в небезопасном режиме. По деньгам это
опаснее падения на старте.

---

## B-15. Admin PUT: реплей маскирует потерю изменений конфига

**Суть.** Для `PUT /admin/*` идемпотентность применяется так же, как для платных операций:
повтор с тем же ключом возвращает закэшированный успешный ответ, не выполняя хендлер. Если
конфиг успел измениться (другим админом, редеплоем с перезаписью файла, откатом),
администратор увидит `200 OK` на изменение, которое не было применено.

**Место.**
- `internal/transport/http/gin/router.go:93-106` — три `PUT` с `IdempotencyMiddleware`
- `internal/transport/http/gin/idempotency_middleware.go:75-80` — реплей без вызова хендлера
- `internal/adapters/revisions/yaml_loader.go` / `internal/adapters/timeouts/memory_store.go` — запись в YAML-файлы (см. A-09: файловое состояние в контейнере)

**Подробное описание (трассировка).**

1. Админ выполняет `PUT /admin/config/timeouts` с ключом `K` → изменения применены,
   ответ закэширован на 24 ч.
2. Pod пересоздан (или конфиг перезаписан из образа — `configs/admin/timeouts.yaml` живёт в
   writable-слое контейнера). Значения вернулись к дефолтным.
3. Админ повторяет тот же `PUT` с тем же `K` (типично для скрипта/CI, где ключ детерминирован
   по содержимому изменения) → middleware отдаёт закэшированный `200`, хендлер не
   вызывается, конфиг остаётся дефолтным.
4. Никакого признака, кроме заголовка `X-Idempotency-Replayed: true`, который CI обычно не
   читает.

Для `PUT` (идемпотентного по определению HTTP) кэширование ответа не даёт ничего полезного:
повторное применение того же состояния безопасно само по себе.

**Предлагаемый багфикс.**

Убрать `IdempotencyMiddleware` с `PUT /admin/*`, оставив только `idempotencyKeyRequired`, если
требование заголовка нужно для аудита:

```go
admin.PUT("/config/timeouts",
    idempotencyKeyRequired(enableIdempotency),
    h.Admin.UpdateTimeouts,
)
```

Если кэш всё же нужен — уменьшить TTL для admin-пространства до минут и логировать каждый
реплей admin-операции отдельной записью аудита.

**Комментарий.** Не про деньги, но про доверие к панели администратора: тихий `200` на
неприменённое изменение хуже явной ошибки. Пункт связан с A-09 и с наблюдением из первичного
обзора о том, что admin-конфиги хранятся в файлах внутри контейнера.

---

## 2. Порядок исправления

Зависимости между пунктами существенны — порядок не произвольный.

1. **B-01 + B-13** (одно изменение: скоупинг + нормализация ключа). База для всего остального,
   правится в одном файле, ничего не ломает.
2. **B-06** (финализация на `WithoutCancel` + метрики store). Без этого любые улучшения кэша
   не работают при отвале клиента. Делать вместе с A-01 — один и тот же приём.
3. **B-08** (`503` вместо `409`, логи, метрика). Дешёво и сразу даёт наблюдаемость, без
   которой остальные фиксы нечем проверить в прод.
4. **B-03 + B-12** (терминальность ответа + отказ от мутации тела). Возвращает
   работоспособность штатного flow частичной оплаты.
5. **B-05** (ключ в bulk-пути) и **B-10** (per-item DLQ, коммит только после DLQ). Строго
   вместе: реплей DLQ безопасен только при наличии ключей.
6. **B-04 + B-07 + B-09** (переход от «кэша ответов» к «журналу операций»: `opState`, фазы,
   контракт с Billing). Самый крупный пункт; требует согласования с Billing.
7. **B-11** (конверт события + ключ партиции). Начинать параллельно, так как требует релиза у
   потребителей; само добавление полей обратно совместимо.
8. **B-14** и **B-15** — гигиена конфигурации и admin-путей, можно в любой момент.

**Напоминание из раздела A, всё ещё актуальное:** HEAD не собирается —
`internal/app/api_app.go:219` вызывает `kafkaadapter.NewConsumer` с тремя аргументами вместо
четырёх (после коммита `e3b3be7`). До исправления этой строки `go build ./...`, `go vet` и
`make test` красные, и ни один фикс этого раздела нельзя проверить в сборке.
