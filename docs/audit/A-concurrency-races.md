# Аудит BFF — Раздел A: Гонки и конкурентность

Ветка: `codebase-analysis` (HEAD `e3b3be7 Kafka DLQ`)
Дата: 2026-08-01
Область: параллельное исполнение, разделяемое состояние, отмена контекста, границы процессов.

---

## 0. Методология

**Статический анализ.** Полностью прочитаны все места, где есть разделяемое состояние или
конкурентность: `internal/adapters/{revisions,timeouts,topupbonus,idempotency,history,events,kafka}`,
`internal/usecase/*`, `internal/transport/http/gin/*`, `internal/transport/kafka/*`,
`internal/app/{api_app,worker_app}.go`, `cmd/{api,worker}/main.go`, `internal/transport/http/gin/server.go`.

Инвентаризация примитивов синхронизации (`grep sync\.|atomic\.|go func`) дала полный список:
8 мьютексов, 2 atomic-счётчика, 2 запускаемые горутины (`idempotency.MemoryStore.cleanup`,
`gintransport.Server.Run → ListenAndServe`). Все мьютексы применены корректно —
классических «забыл RLock» в проекте нет. Все найденные дефекты этого раздела относятся к
трём другим классам:

1. **Отмена контекста** — компенсирующие и освобождающие операции выполняются на том же
   `ctx`, что и основная работа, поэтому при отмене они гарантированно проваливаются.
2. **Гонки по времени (TTL)** — TTL локов и in-flight маркеров меньше максимально возможного
   времени защищаемой операции.
3. **Гонки через границу процессов** — защита реализована in-memory, поэтому при >1 реплике
   её нет вовсе.

**Динамическая проверка.** `go test -race` в этом окружении недоступен: в Go-тулчейне нет
cgo-компилятора (`cgo: C compiler "gcc" not found`), а `-race` требует cgo. Поэтому вместо
детектора гонок написаны **детерминированные пробы** — тесты, которые воспроизводят
проблемный сценарий без зависимости от планировщика. Пробы прогонялись в
`internal/usecase` и после подтверждения удалены (в репозиторий не коммитились).
Результат прогона:

```
=== RUN   TestProbe_A2_EditUnlockUsesCancelledCtx
    ПОДТВЕРЖДЕНО: Unlock вызван с ctx.Err()=context canceled → RedisEditLocker.Unlock
    вернёт ошибку, лок висит до TTL 30s
--- PASS
=== RUN   TestProbe_A6_ReleaseOnCancelledCtxLeavesFundsBlocked
    blocked=2 captured=0 released=0 releaseCalls=1 releaseErr=context canceled
    err=[BILLING_ERROR] all generations failed AND release failed (sagaID=saga-user-1--)
    ПОДТВЕРЖДЕНО: 2 юнитов заблокированы, компенсация Release не выполнена
--- PASS
=== RUN   TestProbe_A5_AIMutatesCallerFieldsMap
    ПОДТВЕРЖДЕНО: входная map вызывающего мутирована: signatureUrl=https://cdn/sig.png
--- PASS
=== RUN   TestProbe_A1_RevisionStoreReturnsAliasedInternals
    ПОДТВЕРЖДЕНО: слайс CalculationChain разделяется со store
    ПОДТВЕРЖДЕНО: map Params разделяется со store
--- PASS
```

**Бюджет времени одного запроса** (используется в расчётах ниже, дефолты из
`internal/config/config.go`):

| Операция | Таймаут | Источник |
|---|---|---|
| BarcodeGen (1 вызов) | 30 000 мс | `EnvBarcodeGenTimeout`, дефолт `30*time.Second` |
| Billing (1 вызов) | 5 000 мс | `EnvBillingTimeout` |
| AI (1 вызов) | 60 000 мс | `EnvAITimeout` |
| Retry BarcodeGen | 3 попытки, паузы 1 s + 3 s = 4 s (третья пауза не берётся) | `generate_usecase.go:19-23` |

Отсюда **худшее время генерации одной единицы** = 3 × 30 s + 4 s = **94 с**.
Худшее время запроса `POST /barcode/generate` при `units=N` ≈
5 s (quote) + chain (до 4 × 30 s) + 60 s (подпись) + 60 s (фото) + 5 s (block) + N × 94 s + 5 s (capture)
= **≈ 255 с + N × 94 с**. Это верхняя граница, но она достижима при деградации BarcodeGen —
именно в этом режиме и ломаются TTL ниже.

---

## 1. Сводка

| ID | Название | Критичность | Класс | Подтверждение |
|---|---|---|---|---|
| A-01 | Компенсация `Release` выполняется на отменённом контексте | **Критично** | Отмена ctx | Проба |
| A-02 | Edit-лок истекает в середине операции и снимается на мёртвом ctx | **Критично** | TTL + отмена ctx | Проба + расчёт |
| A-03 | In-flight маркер идемпотентности (2 мин) короче времени обработки | **Критично** | TTL | Расчёт по коду |
| A-04 | Worker: SIGTERM между списанием и коммитом offset → двойное списание | **Высокая** | Отмена ctx | Трассировка |
| A-05 | In-memory лок и idempotency-store при >1 реплике не защищают ничего | **Высокая** | Границы процессов | Трассировка |
| A-06 | Shutdown бросает незавершённые саги и закрывает Kafka Writer под ними | **Высокая** | Жизненный цикл | Трассировка |
| A-07 | `revisions.MemoryStore` отдаёт наружу алиасы внутренних структур | Средняя | Разделяемое состояние | Проба |
| A-08 | `GenerateUseCase` мутирует входную map вызывающего | Средняя | Разделяемое состояние | Проба |
| A-09 | Файловый I/O под мьютексом на горячем пути каждого HTTP-вызова | Средняя | Contention | Трассировка |
| A-10 | Consumer в API-процессе никогда не стартует → `bulk/wake` всегда врёт | Средняя | Жизненный цикл | Трассировка |
| A-11 | `MockConsumer`: декремент `pending` до обработки, блокирующий `Enqueue` | Низкая | Учёт/API | Трассировка |

---

## A-01. Компенсация `Release` выполняется на отменённом контексте

**Суть.** Все компенсирующие вызовы саги (`Release`, `Capture`) идут с тем же `ctx`, что и
основная работа. Как только контекст отменён — а именно это и происходит в 100 % случаев
таймаута запроса, отвала клиента или SIGTERM — компенсация физически не может выполниться:
`http.Client.Do` вернёт `context.Canceled`, не отправив запрос. Средства остаются
заблокированными в Billing навсегда.

**Место.**
- `internal/usecase/generate_usecase.go:205-211` — `u.billing.Release(ctx, sagaID, generateCount)` (путь «все упали»)
- `internal/usecase/generate_usecase.go:219-221` — `u.billing.Capture(ctx, sagaID, successCount)`
- `internal/usecase/generate_usecase.go:233` — `_ = u.billing.Release(ctx, sagaID, failedCount)` (partial)
- `internal/usecase/edit_usecase.go:78` — `defer u.locker.Unlock(ctx, ...)` (см. также A-02)
- `internal/adapters/billing/http_client.go:196-199` — `c.httpClient.Do(req)` на этом ctx

**Подробное описание (трассировка).**

`APIHandler.Generate` (`api_handler.go:69`) передаёт `c.Request.Context()` в
`generate.Execute`. Gin отменяет этот контекст, когда клиент разрывает соединение или
срабатывает таймаут прокси. Дальше:

1. `Execute` → `u.billing.Block(ctx, …)` — успех, в Billing зарегистрирована ��ага, N юнитов
   заблокированы (`generate_usecase.go:188`).
2. Цикл генерации: `generateWithRetry(ctx, …)`. Внутри `isRetryableBarcodeGenError`
   специально считает `context.Canceled` **не**-retryable (`generate_usecase.go:452-454`),
   поэтому цикл честно и быстро выходит с ошибкой.
3. `successCount == 0` → ветка компенсации: `u.billing.Release(ctx, sagaID, generateCount)`.
4. `billing.HTTPClient.post` → `applyTimeout(ctx)` → `context.WithTimeout(отменённый ctx)`
   даёт уже отменённый дочерний контекст → `httpClient.Do` возвращает
   `context canceled`, ни одного байта в сеть не ушло.
5. Возвращается `BILLING_ERROR 503` с текстом
   `all generations failed AND release failed (sagaID=…)`. Клиент видит 503, деньги
   заблокированы, компенсации нет, автоматического ретрая нет.

Проба воспроизвела это буквально: `blocked=2 captured=0 released=0 releaseErr=context canceled`.

Отдельно отмечу, что тот же дефект в partial-ветке (`generate_usecase.go:233`) хуже:
там ошибка `Release` намеренно проглатывается (`_ =`), так что при отмене контекста
неуспешные юниты «сгорают» полностью бесшумно — ни ошибки клиенту, ни метрики, ни лога.

**Предлагаемый багфикс.**

Ввести отдельный неотменяемый контекст для всех компенсирующих и финализирующих операций
саги. Минимальный вариант — хелпер в `usecase`:

```go
// compensationCtx возвращает контекст, не связанный с отменой запроса:
// значения (trace, request-id) сохраняются, отмена — нет.
func compensationCtx(parent context.Context) (context.Context, context.CancelFunc) {
    return context.WithTimeout(context.WithoutCancel(parent), 15*time.Second)
}
```

`context.WithoutCancel` доступен с Go 1.21 (в проекте Go 1.25 — см. `go.mod`).
Применить во всех точках `Release` / `Capture` / `Unlock` / публикации `billing.saga.*`.

Дополнительно:
- при неудаче `Release`/`Capture` даже на «чистом» контексте — писать в outbox/DLQ
  (`billing.saga.compensation_failed`) и инкрементировать отдельную метрику, чтобы
  зависшие саги были видны в мониторинге, а не только в тексте 503;
- метрику `bff_saga_compensation_failed_total{op="release|capture"}` добавить в
  `internal/metrics/metrics.go` рядом с `PartialSuccessTotal`.

**Комментарий.** Это самый дорогой дефект раздела: он превращает штатное событие (клиент
закрыл вкладку) в потерю денег пользователя без следа в мониторинге. Код при этом написан
аккуратно — автор явно продумал саму сагу и даже описал inconsistent state в комментарии
на `generate_usecase.go:206-208`; проблема ровно в том, что компенсация унаследовала
контекст запроса.

---

## A-02. Edit-лок истекает в середине операции и снимается на мёртвом ctx

**Суть.** Два независимых дефекта на одном локе, оба ведут к повторному использованию
единственного бесплатного редактирования:
(а) `Unlock` вызывается с контекстом запроса, который к моменту `defer` уже может быть
отменён → Redis `DEL` не выполняется;
(б) `editLockTTL = 30 s` меньше худшего времени защищаемой операции (≈ 99 с), поэтому Redis
сам снимет лок, пока первая операция ещё идёт.

**Место.**
- `internal/usecase/edit_usecase.go:78` — `defer func() { _ = u.locker.Unlock(ctx, barcodeID) }()`
- `internal/adapters/idempotency/redis_locker.go:14` — `const editLockTTL = 30 * time.Second`
- `internal/adapters/idempotency/redis_locker.go:52-57` — `Unlock` → `client.Del(ctx, …)`

**Подробное описание (трассировка).**

Комментарий в `redis_locker.go:12-13` утверждает: «30 секунд достаточно для полного цикла:
Block → GeneratePDF417 → PublishBarcodeEdited». Считаем по фактическим таймаутам:

| Шаг | Место | Худшее время |
|---|---|---|
| `history.CheckFreeEdit` | `edit_usecase.go:82` | 5 с (дефолт `timeoutStore.History`) |
| `history.GetBarcode` | `edit_usecase.go:99` | 5 с |
| `billing.Block` | `edit_usecase.go:113` | 5 с (`BILLING_TIMEOUT`) |
| `barcodeGen.GeneratePDF417` | `edit_usecase.go:130` | **30 с** (`BARCODEGEN_TIMEOUT`) |
| `events.PublishBarcodeEdited` | `edit_usecase.go:159` | 5 с (`WriteTimeout` продюсера) |

Итого 50 с > 30 с даже без ретраев. Заметьте: `EditUseCase` вызывает
`barcodeGen.GeneratePDF417` **напрямую**, без `generateWithRetry`, поэтому 94 с здесь не
набегает — но 50 с уже достаточно, чтобы TTL истёк. Сценарий эксплуатации:

1. Атакующий отправляет `POST /api/v1/barcode/{id}/edit` с ключом `K1`. BarcodeGen тормозит.
2. На 31-й секунде Redis удаляет `edit-lock:{id}`.
3. Атакующий отправляет тот же запрос с ключом `K2`. `TryLock` → `OK` (лок свободен),
   `CheckFreeEdit` → всё ещё `true`, потому что `editFlag` ставится **асинхронно**
   консьюмером топика `barcode.edited`, а событие ещё не отправлено.
4. Оба запроса завершаются успехом → два бесплатных редактирования, две регистрации саги
   в Billing с **одинаковым** `sagaID = "edit-" + barcodeID` (`edit_usecase.go:110`).

Дефект (а) усугубляет: даже когда TTL хватает, `defer Unlock(ctx, …)` при отменённом
контексте не снимает лок (подтверждено пробой A2: `ctx.Err()=context canceled`), и лок
живёт полный TTL. Для in-memory локера (`MemoryEditLocker`) обратная крайность — TTL нет
вообще, `Unlock` игнорирует ctx, но лок живёт только в этом процессе (см. A-05).

**Предлагаемый багфикс.**

1. `Unlock` — на неотменяемом контексте (см. A-01):
   ```go
   defer func() {
       ctx, cancel := compensationCtx(ctx)
       defer cancel()
       if err := u.locker.Unlock(ctx, barcodeID); err != nil {
           log.Printf("edit lock: unlock failed barcodeId=%s: %v", barcodeID, err)
       }
   }()
   ```
2. TTL лока вывести из таймаутов, а не из константы: `editLockTTL ≥ history*2 + billing +
   barcodeGen + kafkaWrite + запас`, либо сделать TTL параметром конструктора
   `NewRedisEditLocker(url string, ttl time.Duration)` и считать его в `api_app.go` из
   `cfg.Timeouts`.
3. Лок с fencing-token: писать в значение ключа уникальный `lockID` (uuid) и удалять через
   Lua-скрипт `if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('del',KEYS[1]) end`,
   чтобы «опоздавший» `Unlock` не снял лок, уже захваченный другим запросом.
4. Продлевать лок (watchdog) на время долгого вызова BarcodeGen либо, что проще и
   надёжнее, сделать защиту не таймовой: `editFlag` должен выставляться **синхронно**
   (условная запись в History) до генерации, а не событием после.

**Комментарий.** Сам лок появился как исправление ранее найденной гонки (в коде есть
ссылка на `BFF_Final_Status_Report.md п.3`), и идея верная. Но защита на TTL без fencing и
без продления только сузила окно, а не закрыла его. Корневая причина в том, что признак
«право израсходовано» живёт в другом сервисе и выставляется асинхронно через Kafka —
пока это так, любая блокировка в BFF остаётся эвристикой.

---

## A-03. In-flight маркер идемпотентности (2 мин) короче времени обработки

**Суть.** `Reserve` ставит маркер «запрос в обработке» с жёстко заданным `shortTTL = 2 минуты`.
Запрос `POST /barcode/generate` может легально выполняться дольше. После истечения маркера
параллельный дубликат с тем же `X-Idempotency-Key` проходит проверку и запускает вторую
полноценную сагу — двойное списание.

**Место.**
- `internal/adapters/idempotency/redis_store.go:18` — `const shortTTL = 2 * time.Minute`
- `internal/adapters/idempotency/redis_store.go:76-86` — `Reserve` c `TTL: shortTTL`
- `internal/adapters/idempotency/memory_store.go:74-86` — то же для in-memory
- `internal/transport/http/gin/idempotency_middleware.go:84-113` — фазы Get → Reserve
- `internal/transport/http/gin/idempotency_middleware.go:126-131` — `Set`/`Delete` после `c.Next()`

**Подробное описание (трассировка).**

Расчёт из раздела 0: худшее время `POST /barcode/generate` ≈ 255 с + N × 94 с. Уже при
`units=1` это ≈ 350 с ≫ 120 с. Даже в мягком сценарии (без AI, `units=2`, BarcodeGen отвечает
за 20 с с одним ретраем) получается ≈ 2,5 мин. То есть маркер истекает **в штатной
деградации**, не в экзотике.

Что происходит после истечения:

1. Запрос №1 идёт, маркер `idempotency:K` исчез из Redis по TTL.
2. Клиент (или его retry-политика — а ретрай на таймаут это ровно то, для чего
   идемпотентность и нужна) присылает запрос №2 с тем же `K`.
3. `Get` → не найдено. `Reserve` → `OK`. Middleware пропускает запрос дальше.
4. Второй `Execute` создаёт `sagaID = saga-{userID}-{buildID}-{batchID}` —
   **тот же самый**, что и у первого (`generate_usecase.go:150`). В Billing прилетает
   второй `Block` с идентичным `sagaID`. Поведение Billing в этом случае в BFF никак не
   определено: либо дубль блокировки, либо конфликт саги.
5. BarcodeGen получает `X-Idempotency-Key = K:0, K:1, …`
   (`buildBarcodeGenIdempotencyKey`, `generate_usecase.go:437`), поэтому сами картинки, если
   BarcodeGen честно идемпотентен, не удвоятся — а вот `Capture` вызовется дважды.

Вторая часть дефекта — запись результата: `store.Set(c.Request.Context(), …)` и
`store.Delete(c.Request.Context(), …)` (строки 127 и 130) выполняются **после** `c.Next()`
на контексте запроса. Если клиент отвалился, контекст отменён и обе операции проваливаются,
причём ошибка проглочена (`_ =`). Итог: ответ не закэширован (повтор = повторная оплата)
либо, наоборот, маркер не удалён (повтор = `409 REQUEST_IN_FLIGHT` до истечения TTL).

Третья, более узкая проблема: ключ идемпотентности не включает ни `userID`, ни хэш тела.
Формально это раздел B, здесь фиксирую только как усиливающий фактор — при коллизии ключей
двух пользователей второй получит закэшированный чужой ответ.

**Предлагаемый багфикс.**

1. `shortTTL` не константа, а вычисляемое значение: `ReserveTTL ≥ 2 × худшего времени
   обработки`. Прокинуть в `NewRedisStore(url, ttl, reserveTTL)` и считать в `api_app.go`
   из `cfg.Timeouts` + `maxBarcodeGenRetries`.
2. Продлевать маркер heartbeat-ом на время долгого хендлера (`EXPIRE` каждые 30 с из
   отдельной горутины, привязанной к контексту запроса) — это устраняет и «слишком короткий»,
   и «слишком длинный» TTL одновременно.
3. Ограничить максимальное время обработки самим сервером
   (`http.Server.WriteTimeout` / отдельный `context.WithTimeout` в хендлере) так, чтобы оно
   было заведомо меньше `ReserveTTL`. Это самый простой инвариант:
   `handlerTimeout < reserveTTL`.
4. `Set`/`Delete` в middleware — на неотменяемом контексте (см. A-01) и с логированием
   ошибки вместо `_ =`.

**Комментарий.** В коде видно, что `shortTTL` появился как исправление «Zombie Lock»
(комментарий `redis_store.go:15-19`): раньше маркер жил 24 часа и блокировал клиента после
падения пода. Исправление верное по направлению, но маятник качнулся в другую сторону —
теперь окно потери защиты открывается при любой деградации BarcodeGen. Правильный ответ
здесь не «подобрать число», а связать TTL с таймаутом обработки явным инвариантом.

---

## A-04. Worker: SIGTERM между списанием и коммитом offset → двойное списание

**Суть.** Один и тот же контекст, отменяемый по SIGTERM, используется и для обработки
сообщения (внутри которой происходит списание денег), и для публикации в DLQ, и для
`CommitMessages`. При рестарте пода (деплой, вытеснение, масштабирование) сообщение
переобрабатывается, хотя часть денег по нему уже списана.

**Место.**
- `internal/adapters/kafka/consumer.go:56-83` — цикл `FetchMessage → handler → publishDLQ → CommitMessages`, всё на `ctx`
- `internal/app/worker_app.go:27-30` — `WorkerApp.Run(ctx)` → `consumer.Start(ctx)`
- `cmd/worker/main.go:18` — `signal.NotifyContext(…, SIGINT, SIGTERM)`
- `internal/transport/kafka/bulk_job_handler.go:33-78` — цикл по `msg.Items`

**Подробное описание (трассировка).**

1. `FetchMessage(ctx)` вернул сообщение с `Items: [i1, i2, i3]`.
2. `BulkJobHandler.Handle` последовательно вызывает `generate.Execute` для каждого item.
   Для `i1` сага прошла целиком: `Block` → `GeneratePDF417` → `Capture`. Деньги списаны.
3. Приходит SIGTERM. `ctx` отменён.
4. `i2`: `Block` возвращает `context.Canceled` → `BILLING_ERROR`; `firstErr` заполнен.
   `i3`: то же.
5. `Handle` возвращает `firstErr` → `consumer.go:76` `publishDLQ(ctx, …)` →
   `Producer.Publish` → `writer.WriteMessages(отменённый ctx)` → **ошибка**, сообщение в DLQ
   не попало (только лог на строке 102).
6. `consumer.go:80` `CommitMessages(отменённый ctx)` → **ошибка**, offset не сдвинут
   (только лог на строке 81).
7. Процесс завершается. При старте нового пода то же сообщение читается заново → `i1`
   генерируется и оплачивается **второй раз** (`sagaID` для bulk строится как
   `saga-{userID}-bulk-{jobID}-{batchID}`, т.е. тот же — поведение Billing при повторном
   `Block` с тем же `sagaID` не определено, а `Capture` точно вызовется дважды).

Отдельно: даже без SIGTERM здесь есть смысловая проблема at-least-once — при ошибке
хендлера offset коммитится (строка 80 выполняется всегда, независимо от результата), и
ретраев основного потока нет. Успешно обработанные items тоже не запоминаются, поэтому
любая переобработка сообщения — это переобработка **всего** батча. Это относится и к
разделу B (идемпотентность), здесь фиксирую как усилитель.

**Предлагаемый багфикс.**

1. Разделить контексты в `Consumer.Start`:
   - `ctx` — только для `FetchMessage` (реагирует на shutdown);
   - `workCtx` — `context.WithoutCancel(ctx)` + собственный таймаут для `handler`;
   - `commitCtx` — `context.WithoutCancel(ctx)` + короткий таймаут для `publishDLQ` и
     `CommitMessages`, чтобы offset и DLQ гарантированно дописались перед выходом.
2. После отмены `ctx` — завершать текущее сообщение и только потом выходить из цикла
   (graceful drain), а не бросать его на полуслове.
3. Идемпотентность на уровне item: пробрасывать в `GenerateRequest.IdempotencyKey`
   стабильный ключ вида `bulk:{batchID}:{jobID}` (сейчас
   `bulk_job_handler.go:34-42` его вообще не заполняет), чтобы переобработка не приводила
   к повторной оплате.
4. DLQ-публикацию делать **до** коммита и считать её обязательной: если DLQ недоступен —
   не коммитить offset, иначе сообщение теряется и из основного потока, и из DLQ.

**Комментарий.** DLQ добавили последним коммитом (`e3b3be7`), и он закрывает «застревание»
на плохом сообщении — но текущая склейка «DLQ на том же ctx + коммит всегда» означает,
что при shutdown теряется и то и другое. Заодно напомню, что этот же коммит оставил
`internal/app/api_app.go:219` с прежней 3-аргументной сигнатурой `NewConsumer`, из-за чего
`go build ./...` на HEAD падает — трассировать раздел A пришлось на локально
починенной сборке.

---

## A-05. In-memory лок и idempotency-store при >1 реплике не защищают ничего

**Суть.** Выбор между распределённой и локальной реализацией защиты сделан по наличию
`REDIS_URL`. Если переменная не задана — приложение молча поднимается с in-process
локером и in-process хранилищем идемпотентности. При двух и более репликах API обе защиты
перестают работать полностью: реплики не видят маркеры друг друга.

**Место.**
- `internal/app/api_app.go:68-83` — выбор `RedisStore` vs `MemoryStore` по `os.Getenv(EnvRedisURL)`
- `internal/app/api_app.go:87-99` — то же для `RedisEditLocker` vs `MemoryEditLocker`
- `internal/config/config.go:155-157` — `Redis.URL` имеет дефолт `redis://redis:6379/0`
- `internal/adapters/idempotency/memory_locker.go:23-32` — лок в `map` процесса

**Подробное описание (трассировка).**

Обратите внимание на асимметрию: `cfg.Redis.URL` **всегда** заполнен (дефолт
`redis://redis:6379/0`), но ветвление смотрит не на `cfg`, а на «сырой»
`os.Getenv(config.EnvRedisURL)`. То есть конфиг говорит «Redis есть», а сборка приложения
считает, что его нет. Забыть выставить `REDIS_URL` в манифесте — единственное, что нужно,
чтобы получить:

- `MemoryEditLocker` в каждой реплике → `TryLock` в реплике A ничего не знает о реплике B
  → N параллельных `POST /barcode/{id}/edit`, распределённых балансировщиком по N реплик,
  дают N бесплатных редактирований. Это ровно та гонка, которую лок и должен был закрыть;
- `idempotency.MemoryStore` в каждой реплике → повторный запрос с тем же
  `X-Idempotency-Key`, попавший в другую реплику, выполняется как новый → двойное списание;
- при рестарте одной реплики её маркеры исчезают целиком.

Никакого предупреждения в логах об этом нет — только информационные
`"edit locker: REDIS_URL not set, using in-memory locker"` и
`"idempotency: REDIS_URL not set, using in-memory store"` уровня `log.Printf`.

**Предлагаемый багфикс.**

1. Ввести `APP_ENV`/`ENVIRONMENT`. При значении `production`/`staging` отсутствие
   `REDIS_URL` — фатальная ошибка старта (`log.Fatalf`), а не переход на in-memory.
   То же правило применить ко всем fail-open переходам на моки (`BILLING_URL`,
   `BARCODEGEN_URL`, `KAFKA_BROKERS`) — это отдельный пункт раздела E, но чинится одним
   механизмом.
2. Убрать расхождение между `cfg.Redis.URL` и `os.Getenv`: либо снять дефолт из конфига
   (пусть пустая строка означает «нет Redis»), либо ветвиться по `cfg.Redis.URL != ""`.
   Сейчас два источника истины противоречат друг другу.
3. Readiness-проба должна проверять доступность Redis, если он объявлен обязательным.

**Комментарий.** Это метапроблема, а не единичная гонка: она обесценивает сразу два
корректно написанных механизма (A-02 и A-03). Дефолты «удобны для локальной разработки»,
но цена такого удобства — тихая деградация в проде без единого ERROR в логах.

---

## A-06. Shutdown бросает незавершённые саги и закрывает Kafka Writer под ними

**Суть.** `Server.Run` даёт незавершённым запросам ровно 5 секунд, после чего возвращает
`nil`, а вызывающий сразу закрывает Kafka-продюсер. Хендлеры, не успевшие за 5 секунд
(а по расчёту раздела 0 они легко идут минуты), продолжают работать «в воздухе» и пишут в
закрытый Writer.

**Место.**
- `internal/transport/http/gin/server.go:26-42` — `Run` с `context.WithTimeout(…, 5*time.Second)`
- `cmd/api/main.go:19-24` — `defer application.Close()` + `application.Run(ctx)`
- `internal/app/api_app.go:41-49` — `Close()` → `producer.Close()`
- `internal/adapters/kafka/producer.go:59-61` — `writer.Close()`

**Подробное описание (трассировка).**

1. SIGTERM → `ctx.Done()` в `Server.Run` (`server.go:32`).
2. `httpServer.Shutdown(shutdownCtx)` с таймаутом 5 с. `Shutdown` **не** отменяет контексты
   активных запросов — он лишь ждёт их завершения; по истечении таймаута возвращает
   `context.DeadlineExceeded` и перестаёт ждать. Результат `Shutdown` игнорируется (`_ =`).
3. `Run` возвращает `nil` → `main` выходит из `if` → срабатывает `defer application.Close()`
   → `producer.Close()`.
4. В этот момент сага, начатая 10 секунд назад, всё ещё в цикле генерации. Она доходит до
   `u.events.PublishBarcodeGenerated(...)` → `writer.WriteMessages` на **закрытом** Writer →
   `io.ErrClosedPipe`. Ошибка проглочена (`_ =`, `generate_usecase.go:288`), событие
   `barcode.generated` потеряно: деньги списаны (`Capture` прошёл), в History записи нет.
5. Затем процесс просто завершается, обрывая всё остальное.

Дополнительно в том же файле: `http.Server` создан без `ReadHeaderTimeout`, `ReadTimeout`,
`WriteTimeout`, `IdleTimeout` — то есть верхней границы времени жизни запроса нет вообще
(это же питает A-03) и открыт Slowloris.

**Предлагаемый багфикс.**

1. Согласовать бюджеты: `shutdownTimeout ≥ WriteTimeout ≥ худшее время саги`, вынести в
   конфиг (`SHUTDOWN_TIMEOUT`, дефолт 30–60 с) вместо литерала `5*time.Second`.
2. Не игнорировать результат `Shutdown`: при `DeadlineExceeded` — залогировать
   как ERROR с количеством брошенных соединений.
3. Порядок закрытия: сначала дождаться Shutdown, затем `producer.Close()`. Сейчас порядок
   формально верный, но из-за таймаута фактически нарушается — исправляется п.1 плюс
   явным `sync.WaitGroup` на активные саги.
4. Задать таймауты сервера:
   ```go
   httpServer: &http.Server{
       Addr:              ":" + port,
       Handler:           router,
       ReadHeaderTimeout: 10 * time.Second,
       ReadTimeout:       30 * time.Second,
       WriteTimeout:      writeTimeout, // > худшего времени саги
       IdleTimeout:       120 * time.Second,
   }
   ```

**Комментарий.** 5 секунд — типичный «скопированный» дефолт, безобидный для CRUD-сервиса
и опасный для сервиса, где один запрос может минутами держать распределённую транзакцию.
В связке с A-01 это даёт худший исход: сага брошена, компенсация невозможна, событие
потеряно.

---

## A-07. `revisions.MemoryStore` отдаёт наружу алиасы внутренних структур

**Суть.** `GetConfig` и `ListConfigs` возвращают `domain.RevisionConfig` по значению, но
внутри структуры лежат ссылочные типы: слайс `CalculationChain` и map `Params` в каждом его
элементе. Копируется только заголовок слайса — данные остаются общими с хранилищем.
Мьютекс защищает доступ к map `configs`, но не защищает то, что из него вынесли.

**Место.**
- `internal/adapters/revisions/memory_store.go:96-105` — `ListConfigs`
- `internal/adapters/revisions/memory_store.go:107-116` — `GetConfig`
- `internal/domain/revision_config.go` — `ChainEntry.Params map[string]any`
- Читатели: `internal/usecase/chain_executor.go:60` (итерация по `cfg.CalculationChain`),
  `chain_executor.go:86` (передача `step.Params` в BarcodeGen),
  `internal/usecase/generate_usecase.go:105` (`validateMinimumSet`)
- Писатель: `internal/adapters/revisions/memory_store.go:118-133` — `UpdateConfig`

**Подробное описание (трассировка).**

Проба подтвердила аliasing напрямую: значение, полученное из `GetConfig`, было изменено
снаружи (`cfg.CalculationChain[0].Field = "HACKED"`, `Params["injected"] = true`), и
повторный `GetConfig` вернул уже изменённые данные — то есть внешняя мутация попала внутрь
хранилища, минуя мьютекс.

Почему это пока не «стреляет»: `UpdateConfig` (`memory_store.go:127-128`) не мутирует
существующий слайс, а **заменяет** его целиком (`updated.CalculationChain = req.CalculationChain`).
Горутина, уже держащая старый слайс, продолжает читать старый (иммутабельный) массив —
это безопасно. Аналогично `LoadFromDir` создаёт новые слайсы.

То есть сегодня это латентный дефект: гонки нет ровно потому, что никто не мутирует
структуры на месте. Но контракт API это не гарантирует, и любое будущее изменение
вида «обновить один шаг цепочки» или «дописать Params» создаёт настоящую гонку
чтения/записи map без синхронизации — с падением процесса
(`concurrent map read and map write` — это фатальная ошибка рантайма, не паника, которую
можно поймать в `recover`).

**Предлагаемый багфикс.**

Возвращать глубокие копии из `GetConfig`/`ListConfigs`:

```go
func cloneConfig(cfg domain.RevisionConfig) domain.RevisionConfig {
    out := cfg
    out.RequiredInputFields = slices.Clone(cfg.RequiredInputFields)
    out.CalculationChain = make([]domain.ChainEntry, len(cfg.CalculationChain))
    for i, e := range cfg.CalculationChain {
        e.DependsOn = slices.Clone(e.DependsOn)
        e.Params = maps.Clone(e.Params)
        out.CalculationChain[i] = e
    }
    return out
}
```

Симметрично клонировать входные данные в `UpdateConfig`/`LoadFromDir` (иначе тело
HTTP-запроса остаётся связанным с состоянием хранилища). То же самое применить к
`GetSchema` (`memory_store.go:84-92`) — там та же схема с `Fields`/`Groups`/`Options`.
Альтернатива, если копирование на горячем пути смущает: перейти на неизменяемые снапшоты
через `atomic.Pointer[configSnapshot]` — тогда читатели вообще не берут мьютекс.

**Комментарий.** Классическая ловушка «структура скопирована по значению, значит она
изолирована». Здесь она пока обезврежена стилем записи (замена вместо мутации), но
полагаться на это нельзя — инвариант нигде не зафиксирован ни в комментарии, ни в тесте.

---

## A-08. `GenerateUseCase` мутирует входную map вызывающего

**Суть.** Когда `ChainExecutor` не подключён или `req.Fields` пуст, `resolvedFields`
становится **тем же самым** map, что пришёл от вызывающего. AI-шаг затем пишет в него
`signatureUrl` и `photoUrl`, изменяя структуру, которой владеет вызывающий код. Тот же map
без копирования уходит в N Kafka-событий.

**Место.**
- `internal/usecase/generate_usecase.go:132-135` — `resolvedFields := req.Fields`
- `internal/usecase/generate_usecase.go:137-146` — условие `if u.chain != nil && len(req.Fields) > 0`
- `internal/usecase/generate_usecase.go:176`, `:185` — `resolvedFields["signatureUrl"] = …`, `["photoUrl"] = …`
- `internal/usecase/generate_usecase.go:283` — `Fields: resolvedFields` в каждом событии цикла
- Вызывающие: `internal/transport/http/gin/api_handler.go:69` (map из `ShouldBindJSON`),
  `internal/transport/kafka/bulk_job_handler.go:41` (`Fields: item.Fields` — map из тела Kafka-сообщения)

**Подробное описание (трассировка).**

Проба подтвердила: после `Execute` в переданной вызывающим map появился ключ
`signatureUrl`. Условие срабатывания — `u.chain == nil || len(req.Fields) == 0`:

- в проде `chain` всегда подключён (`api_app.go:194`, `worker_app.go:118`), но условие
  включает и `len(req.Fields) == 0`, а при пустых полях копия тоже не делается — там,
  правда, и мутировать нечего, кроме созданного тут же пустого map;
- реальный риск в `BulkJobHandler`: `item.Fields` — часть распарсенного
  `domain.BulkJobMessage`. Если завтра обработка items станет параллельной (что естественно
  для батчей — это первый кандидат на оптимизацию), несколько горутин начнут писать
  `signatureUrl` в разные map, но `resolvedFields` каждой из них может ссылаться на общие
  вложенные структуры сообщения;
- второй риск — публикация: один и тот же `resolvedFields` передаётся во все N событий
  `BarcodeGeneratedEvent` (`generate_usecase.go:283`). Сейчас `KafkaPublisher.Publish`
  сериализует синхронно, поэтому гонки нет. Но любой асинхронный/буферизованный публикатор
  (или mock, который просто складывает события в слайс, как `MockPublisher` делает для
  `BarcodeEdited`) получит N событий, разделяющих один map, — и последующая мутация
  «протечёт» во все ранее опубликованные.

**Предлагаемый багфикс.**

1. Всегда работать с копией, не завися от наличия chain:
   ```go
   resolvedFields := make(map[string]any, len(req.Fields)+2)
   for k, v := range req.Fields {
       resolvedFields[k] = v
   }
   if u.chain != nil && len(req.Fields) > 0 {
       chainResult, chainErr := u.chain.Execute(ctx, req.Revision, req.Fields)
       …
       resolvedFields = chainResult.Fields // ChainExecutor уже возвращает свою копию
   }
   ```
2. В цикле публикации отдавать каждому событию свою копию (`maps.Clone(resolvedFields)`),
   либо явно задокументировать, что событие владеет map только до возврата из `Publish`.
3. Закрепить контракт тестом: «`Execute` не изменяет `req.Fields`».

**Комментарий.** `ChainExecutor.Execute` копирует вход честно и аккуратно
(`chain_executor.go:47-50`) — то есть автор про этот класс проблем знает. Дефект возник
именно в ветке «chain отключён», которую в проде не проходят, поэтому он и не всплыл. Пока
это гигиена, а не инцидент; станет инцидентом ровно в тот день, когда обработку bulk-items
распараллелят.

---

## A-09. Файловый I/O под мьютексом на горячем пути каждого HTTP-вызова

**Суть.** `timeouts.MemoryStore.Set` выполняет `os.WriteFile` **под** эксклюзивным
`sync.Mutex`. Тот же мьютекс бе��ут на `RLock` все downstream-клиенты, причём на каждый
свой запрос. Один админский `PUT /admin/config/timeouts` с медленным диском блокирует
все исходящие вызовы Billing/BarcodeGen/AI/History во всём процессе.

**Место.**
- `internal/adapters/timeouts/memory_store.go:42-50` — `Set` под `Lock`, внутри `persistLocked`
- `internal/adapters/timeouts/memory_store.go:74-85` — `persistLocked` → `os.WriteFile`
- `internal/adapters/timeouts/memory_store.go:52-72` — `LoadFromFile` под `Lock`, внутри `MkdirAll` + `ReadFile`
- `internal/adapters/topupbonus/memory_store.go:37-45, 47-68` — тот же паттерн
- `internal/adapters/revisions/memory_store.go:118-133` — `UpdateConfig` → `persistConfigLocked` → `WriteFile` под `Lock`
- Читатели горячего пути: `billing/http_client.go:38-46`, `barcodegen/http_client.go:41-49`,
  `ai/http_client.go`, `history/http_client.go` — все вызывают `timeoutStore.Get(ctx)` в
  `applyTimeout` на **каждый** запрос

**Подробное описание (трассировка).**

Путь чтения: `GenerateUseCase.Execute` → `billing.Block` → `HTTPClient.post` →
`applyTimeout(ctx)` → `c.timeoutStore.Get(ctx)` → `RLock`. При `units=N` только генерация
даёт N вызовов BarcodeGen, каждый со своим `Get`. То есть `RLock` берётся десятки раз за
запрос и тысячи раз в секунду под нагрузкой.

Путь записи: `PUT /admin/config/timeouts` → `AdminHandler.UpdateTimeouts` →
`timeouts.MemoryStore.Set` → `Lock` → `yaml.Marshal` → `os.WriteFile` → `Unlock`.
Пока идёт `WriteFile` (на сетевом или дросселированном томе это десятки–сотни мс, при
`fsync`-давлении — секунды), все читатели стоят в очереди за `RLock`. Go-мьютексы к тому же
не отдают предпочтение читателям: ожидающий писатель блокирует новые `RLock`, так что
эффект не «немного медленнее», а «полная остановка исходящего трафика».

Тот же паттерн у `revisions.MemoryStore.UpdateConfig`: под `Lock` пишется YAML, а
`ChainExecutor.Execute` берёт `RLock` на каждый `GetConfig` (`chain_executor.go:41`).

Дополнительно `LoadFromFile` создаёт каталоги и читает файл под `Lock` — на старте это
безвредно, но метод публичный и может быть вызван для перезагрузки конфига в рантайме.

**Предлагаемый багфикс.**

1. Вынести I/O из-под мьютекса: под `Lock` только подготовить/зафиксировать значение,
   запись на диск делать после `Unlock`:
   ```go
   func (s *MemoryStore) Set(_ context.Context, t domain.ServiceTimeouts) error {
       s.mu.Lock()
       prev, path := s.current, s.path
       s.current = t
       s.mu.Unlock()
       if err := persist(path, t); err != nil {
           s.mu.Lock(); s.current = prev; s.mu.Unlock() // откат
           return err
       }
       return nil
   }
   ```
2. Писать атомарно: `WriteFile` во временный файл + `os.Rename`. Сейчас
   `os.WriteFile` усекает файл на месте, поэтому падение процесса посередине оставляет
   обрезанный YAML, который при следующем старте не распарсится (а `LoadFromFile` в
   `api_app.go:63` только логирует warning и молча уезжает на дефолты).
3. Для read-mostly состояния (`timeouts`, `topupbonus`, `revisions`) перейти на
   `atomic.Pointer` со снапшотом — тогда горячий путь становится вообще безблокировочным.

**Комментарий.** Сюда же примыкает архитектурная проблема из раздела F: конфиг лежит в
файлах внутри контейнера, поэтому админская правка не переживает рестарт и не
распространяется на другие реплики. Исправлять по-хорошему нужно вместе — переносом
admin-конфига в Redis/БД; тогда и I/O под мьютексом исчезает сам.

---

## A-10. Consumer в API-процессе никогда не стартует → `bulk/wake` всегда врёт

> **⇔ Дубль. Канонично: [F-07](F-operations-observability.md). В итоговом счёте учитывается один раз под F-07.** Тот же дефект (`Start()` не вызывается в `APIApp.Run`), угол раздела A — жизненный цикл компонента и риск конкуренции за партиции при «наивном» исправлении. Независимым дефектом не считается.

**Суть.** API-процесс создаёт полноценный Kafka-консьюмер (или mock), но `Start()` для
него не вызывается никогда: `APIApp.Run` запускает только HTTP-сервер. Эндпоинт
`POST /api/v1/bulk/wake`, чей смысл — подтвердить Bulk Service, что BFF читает Kafka,
всегда возвращает `pendingMessages: 0`.

**Место.**
- `internal/app/api_app.go:211-223` — создание `bulkConsumer`
- `internal/app/api_app.go:33-35` — `APIApp.Run` → только `a.server.Run(ctx)`
- `internal/transport/http/gin/api_handler.go:157-163` — `BulkWake` → `h.consumer.PendingCount()`
- `internal/adapters/kafka/consumer.go:108-116` — `PendingCount` → `reader.Stats().Lag`
- `internal/adapters/kafka/mock_consumer.go:46-48` — `PendingCount` → `pending.Load()`

**Подробное описание (трассировка).**

Реальный путь: `kafka.NewReader` c `GroupID` присоединяется к consumer group **лениво** —
только при первом `FetchMessage`. `Start()` не вызывается, значит группа не джойнится,
партиции не назначаются, `Stats().Lag` остаётся нулём. `PendingCount()` → 0 всегда.
(Хорошая новость: партиции у воркера этот reader не отбирает — ленивость его спасает.
Но если кто-то «исправит» это, добавив `go bulkConsumer.Start(ctx)` в `APIApp.Run`, API и
worker начнут конкурировать за партиции с одним `group.id = bff-bulk-worker` — и часть
батчей будет обрабатываться в API-процессе, где нет ни DLQ (см. A-04: в
`api_app.go:219` DLQ-аргумент вообще не передаётся), ни ожидания завершения при shutdown.)

Mock-путь: `MockConsumer.pending` инкрементируется только в `Enqueue`, а `Enqueue` в
production-коде не вызывается ни разу (grep по репозиторию: только `mock_consumer.go` и
тесты). Тоже всегда 0.

Итог: эндпоинт синтаксически работает, семантически бесполезен — он не различает
«BFF читает Kafka» и «BFF вообще не подключён к Kafka». Плюс в API-процессе висит
неиспользуемый `kafka.Reader` со своим пулом соединений.

**Предлагаемый багфикс.**

Определиться с назначением эндпоинта, варианты по возрастанию честности:

1. **Минимально:** убрать консьюмера из API-процесса совсем, а `bulk/wake` превратить в
   health-check Kafka: `kafka.DialContext` + запрос метаданных топика `bulk.tasks`,
   возвращать `{"status":"awake","brokersReachable":true}` без поля `pendingMessages`.
2. **Честный лаг:** отдавать реальный лаг consumer group воркера через Admin API Kafka
   (`OffsetFetch`/`ListOffsets` по `group.id`), а не через локальный reader.
3. **Если консьюмер в API нужен по замыслу:** запускать его в `APIApp.Run` через
   `errgroup`, выделить **отдельный** `group.id` (например `bff-bulk-api`), передать DLQ и
   учесть его в graceful shutdown.

**Комментарий.** Это не гонка в узком смысле, но проблема того же класса «жизненный цикл
компонентов»: объект сконструирован, зарегистрирован в хендлере и выглядит работающим,
хотя его основной метод никто не вызывает. Тестами это не поймалось, потому что тесты
проверяют форму ответа (`status`, `pendingMessages`), а не то, что счётчик отражает
реальность.

---

## A-11. `MockConsumer`: декремент `pending` до обработки, блокирующий `Enqueue`

**Суть.** Два мелких дефекта в mock-консьюмере: счётчик `pending` уменьшается **до**
вызова хендлера, поэтому обрабатываемое сообщение не учитывается нигде; `Enqueue`
блокируется навсегда при заполненном буфере, уже увеличив счётчик.

**Место.**
- `internal/adapters/kafka/mock_consumer.go:37-42` — `case msg := <-c.messages: c.pending.Add(-1)` перед `c.handler(...)`
- `internal/adapters/kafka/mock_consumer.go:51-54` — `Enqueue`: `pending.Add(1)`, затем блокирующая отправка в канал

**Подробное описание (трассировка).**

`Start` читает из канала, сразу делает `pending.Add(-1)` и только потом вызывает
`c.handler(ctx, msg)`, который может выполняться минуты (полная сага на каждый item). В
этом окне `PendingCount()` показывает 0, хотя работа не завершена — то есть даже в
dev-режиме `bulk/wake` не показывает «есть незавершённая работа».

`Enqueue` при `bufSize = 256` (значение из `api_app.go:222` и `worker_app.go:137`) на
257-м сообщении блокируется на отправке в канал, уже инкрементировав `pending`. Если бы он
вызывался из HTTP-хендлера, это была бы зависшая горутина с растущим `pending`; сейчас он
вызывается только из тестов, поэтому влияние ограничено — но публичный метод без
`select { case … default: }` или контекста остаётся ловушкой.

Здесь же отмечу асимметрию контрактов: `MockConsumer.PendingCount` считает «сообщения в
буфере», а `Consumer.PendingCount` — «lag consumer group». Это два разных числа под одним
именем в одном интерфейсе `ports.BulkJobConsumer` (`ports.go:110-116`) — деталь для
раздела D, но она напрямую делает `bulk/wake` неинтерпретируемым.

**Предлагаемый багфикс.**

```go
case msg := <-c.messages:
    if err := c.handler(ctx, msg); err != nil { … }
    c.pending.Add(-1) // после обработки

func (c *MockConsumer) Enqueue(ctx context.Context, msg domain.BulkJobMessage) error {
    select {
    case c.messages <- msg:
        c.pending.Add(1)
        return nil
    case <-ctx.Done():
        return ctx.Err()
    default:
        return errors.New("bulk mock consumer: buffer full")
    }
}
```

Плюс зафиксировать в `ports.BulkJobConsumer` точную семантику `PendingCount` (что именно
считаем: непрочитанные, необработанные или lag) и привести обе реализации к ней.

**Комментарий.** Низкий приоритет сам по себе, но чинить стоит вместе с A-10: пока
семантика счётчика не определена, любые «улучшения» wake-эндпоинта останутся угадыванием.

---

## 2. Порядок исправления

Внутри раздела A зависимости такие:

1. **A-05** (strict-режим конфигурации) — первым, иначе исправления A-02 и A-03 не
   действуют в проде: они защищают только при наличии Redis.
2. **A-01** (`compensationCtx`) — общий механизм, который затем переиспользуют A-02
   (`Unlock`), A-03 (`Set`/`Delete`) и A-04 (DLQ + commit).
3. **A-06** (таймауты сервера + shutdown) — задаёт верхнюю границу времени запроса, без
   которой невозможно корректно выбрать TTL в A-02 и A-03.
4. **A-02**, **A-03**, **A-04** — TTL и границы контекстов, уже на готовых механизмах.
5. **A-07**, **A-08**, **A-09** — гигиена разделяемого состояния, независимы друг от друга.
6. **A-10**, **A-11** — вместе, после того как определена семантика `PendingCount`.

Отдельно, вне очереди: `internal/app/api_app.go:219` — сборка HEAD не проходит
(`NewConsumer` вызван с 3 аргументами вместо 4). Пока это не исправлено, ни один из пунктов
выше нельзя проверить на `cmd/api`.

Также для полноценной проверки этого раздела в CI нужен `-race`: сейчас его нельзя
запустить (нет cgo-компилятора в образе). Добавить в CI-образ `gcc` и шаг
`go test -race ./...` — без него гонки классов A-07/A-08 не поймаются автоматически.
