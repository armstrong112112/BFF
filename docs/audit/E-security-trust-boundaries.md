# Раздел E — Безопасность и границы доверия

> Аудит кодовой базы `github.com/ikermy/BFF`, ветка `codebase-analysis` (HEAD `e3b3be7`).
> Метод: чтение кода + детерминированные пробы (`httptest`, реальные `AdminJWTMiddleware`,
> `APIHandler`, `LocalValidator`, `history.MockClient`). Пробы прогонялись локально
> (Go 1.25 в `/tmp/go`), вывод приведён дословно, файлы проб в репозиторий не коммитились.

## E.0. Сводка

| ID | Название | Критичность | Статус проверки |
|----|----------|-------------|-----------------|
| E-01 | IDOR: чтение любого баркода по ID без проверки владельца | 🔴 Критично | Подтверждено пробой |
| E-02 | IDOR: редактирование чужого баркода и сжигание чужой free-edit квоты | 🔴 Критично | Подтверждено трассировкой |
| E-03 | Дефолтный `JWT_SECRET` позволяет подделать токен любого пользователя | 🔴 Критично | Подтверждено пробой |
| E-04 | Fail-open аутентификация: без ENV поднимается `auth.MockClient`, принимающий любой токен | 🔴 Критично | Подтверждено чтением кода |
| E-05 | `ENABLE_LEGACY_AUTH=false` схлопывает всех пользователей в одного `anonymous` | 🔴 Критично | Подтверждено чтением кода |
| E-06 | Fail-open биллинг: без `BILLING_URL` поднимается `billing.MockClient` — бесплатная генерация | 🔴 Критично | Подтверждено чтением кода |
| E-07 | `/admin` и `/internal` защищены статическим shared secret, а не JWT; нет `role=admin` | 🟠 Высокая | Подтверждено пробой |
| E-08 | Пустой `expectedToken` в `AdminJWTMiddleware`/`ServiceJWTMiddleware` = полный обход | 🟠 Высокая | Подтверждено пробой |
| E-09 | JWT без claim `exp` принимается бессрочно; нет проверки `iss`/`aud` | 🟠 Высокая | Подтверждено пробой |
| E-10 | Один и тот же `INTERNAL_SERVICE_JWT` защищает `/internal/*` и `/api/v1/bulk/wake` | 🟠 Высокая | Подтверждено чтением кода |
| E-11 | Нет лимита размера тела запроса — 8 МБ JSON читается и парсится целиком | 🟠 Высокая | Подтверждено пробой |
| E-12 | Нет rate limiting ни на одном эндпоинте | 🟠 Высокая | Подтверждено чтением кода |
| E-13 | PII (имя, фамилия, DOB) и URL артефактов пишутся в stdout-логи | 🟡 Средняя | Подтверждено чтением кода |
| E-14 | `/metrics` и `/health` доступны без аутентификации | 🟡 Средняя | Подтверждено чтением кода |
| E-15 | Нет CORS-политики и security-заголовков | 🟡 Средняя | Подтверждено чтением кода |
| E-16 | Детали внутренних ошибок утекают клиенту через `NewBarcodeGenError(err)` | 🟡 Средняя | Подтверждено чтением кода |
| E-17 | `BILLING_INTERNAL_API_KEY` с дефолтом `dev-billing-key` | 🟡 Средняя | Подтверждено чтением кода |
| E-18 | Path traversal в `PUT /admin/revisions/:revision` — **не воспроизводится** | ⚪ Проверено, безопасно | Негативный результат |
| E-19 | SSRF через `signatureUrl`/`photoUrl` — **в BFF отсутствует** | ⚪ Проверено, безопасно | Негативный результат |
| E-20 | JWT alg confusion (`alg=none`, RS256-подмена) — **отклоняется корректно** | ⚪ Проверено, безопасно | Негативный результат |

Итого: 6 критичных, 5 высоких, 5 средних, 3 негативных результата (проверено — уязвимости нет).

### Ключевой вывод раздела

Три критичных дефекта складываются в **полный обход разграничения доступа**:
`E-03` (подделка токена) + `E-01`/`E-02` (нет проверки владельца) позволяют неаутентифицированному
злоумышленнику прочитать и изменить документы любого пользователя. Учитывая предметную область
(персональные данные из водительских удостоверений: ФИО, дата рождения, фото, подпись),
это утечка PII/чувствительных данных, а не абстрактная «дырка в API».

---

## E-01. IDOR: чтение любого баркода по ID без проверки владельца

### Суть
`GET /api/v1/barcode/:id` возвращает полную запись баркода — ФИО, дату рождения, URL изображения —
любому аутентифицированному пользователю, который знает или угадал ID. Проверки владельца нет
вообще: `userInfo` в обработчике даже не запрашивается.

### Место
- `internal/transport/http/gin/api_handler.go:135-150` — `APIHandler.GetBarcode`
- `internal/transport/http/gin/router.go:76` — регистрация маршрута
- `internal/domain/barcode.go:69-79` — `BarcodeRecord` содержит поле `UserID`

### Подробное описание

Тело обработчика целиком:

```go
func (h *APIHandler) GetBarcode(c *gin.Context) {
	barcodeID := c.Param("id")
	if barcodeID == "" {
		RespondError(c, domain.NewValidationError("barcode id is required"))
		return
	}

	record, err := h.history.GetBarcode(c.Request.Context(), barcodeID)
	if err != nil {
		RespondError(c, domain.NewBarcodeGenError(err))
		return
	}
	c.JSON(http.StatusOK, record)      // ← отдаётся как есть
}
```

Сравните с соседними обработчиками того же файла: `GetQuote` (строка 57), `Generate` (строка 73),
`EditBarcode` (строка 112) — все вызывают `GetUserInfo(c)`. В `GetBarcode` вызова нет: личность
вызывающего не участвует в обработке ни на одном шаге. Единственная защита — `UserJWTMiddleware`,
которая отвечает на вопрос «пользователь аутентифицирован?», но не «этот баркод его?».

При этом `domain.BarcodeRecord` содержит `UserID` (строка 71), то есть данные для проверки
уже приходят из History Service — их просто никто не сверяет.

Что утекает в ответе (по структуре `BarcodeRecord`):

| Поле | Содержимое |
|------|-----------|
| `userId` | ID владельца — позволяет перебирать связанные записи |
| `fields` | `firstName`, `lastName`, `dateOfBirth`, `eyeColor`, адрес, фото, подпись |
| `barcodeUrl` | Прямая ссылка на готовое изображение документа |
| `revision` | Тип документа (`US_CA_08292017`) |
| `editFlag` | Использовано ли право бесплатной правки |

Комментарий в коде («Возвращает поля существующего баркода для Remake») объясняет назначение
эндпоинта — заполнение формы перед перегенерацией — и именно поэтому он отдаёт весь набор полей.
Назначение легитимное, отсутствие проверки владельца — нет.

### Трассировка

Проба поднимает **реальный** `APIHandler` с `history.MockClient` и подставляет в контекст
`attacker-user`:

```go
hist := history.NewMockClient()
h := NewAPIHandler(nil, nil, nil, nil, nil, nil, nil, hist)
r.Use(func(c *gin.Context) {
    c.Set(ContextKeyUserInfo, domain.UserInfo{UserID: "attacker-user"})
    c.Next()
})
r.GET("/api/v1/barcode/:id", h.GetBarcode)
```

Результат:

```
PROBE E-03: caller=attacker-user status=200 record.userId=mock-user-id
PROBE E-03: returned fields=map[dateOfBirth:1990-05-15 firstName:JOHN lastName:DOE]
            barcodeUrl=https://cdn.example.com/barcodes/victim-barcode-42.png
PROBE E-03 CONFIRMED: выдана запись чужого владельца (mock-user-id), проверки owner нет
```

`caller=attacker-user`, `record.userId=mock-user-id` — идентичности разные, ответ `200`
с полным набором PII.

Дополнительный усиливающий фактор: формат `barcodeID` нигде не ограничен. Если History Service
использует последовательные или предсказуемые ID, перебор тривиален; при UUID нужен известный ID,
но он попадает в ответ `POST /barcode/generate`, в события Kafka `barcode.generated`
и в логи (см. E-13).

### Предлагаемый багфикс

Сверять владельца сразу после получения записи и отвечать `404` (а не `403`), чтобы не
подтверждать существование чужого ID:

```go
func (h *APIHandler) GetBarcode(c *gin.Context) {
	barcodeID := c.Param("id")
	if barcodeID == "" {
		RespondError(c, domain.NewValidationError("barcode id is required"))
		return
	}

	userInfo, ok := GetUserInfo(c)
	if !ok || userInfo.UserID == "" {
		RespondError(c, &domain.AppError{
			Code: domain.ErrCodeUnauthorized, HTTPStatus: 401, Message: "unauthenticated",
		})
		return
	}

	record, err := h.history.GetBarcode(c.Request.Context(), barcodeID)
	if err != nil {
		RespondError(c, domain.NewBarcodeGenError(err))
		return
	}

	// Проверка владельца: 404 вместо 403, чтобы не раскрывать существование чужого ID.
	if record.UserID != userInfo.UserID {
		log.Printf("audit: ownership denied barcode=%s caller=%s", barcodeID, userInfo.UserID)
		RespondError(c, &domain.AppError{
			Code: domain.ErrCodeNotFound, HTTPStatus: 404, Message: "barcode not found",
		})
		return
	}

	c.JSON(http.StatusOK, record)
}
```

Дополнительно (defense in depth): передавать `userID` в `HistoryClient.GetBarcode` и фильтровать
на стороне History Service, чтобы проверка не зависела только от BFF. Это меняет порт:

```go
// internal/ports/ports.go
GetBarcode(ctx context.Context, userID, barcodeID string) (domain.BarcodeRecord, error)
```

Регрессионный тест обязателен: запрос от `user-A` к записи `user-B` → `404`.

### Комментарий

Это самый серьёзный дефект из найденных за все разделы. Причина, судя по структуре кода, —
разделение ответственности «по слоям»: аутентификацию делает middleware, авторизацию объекта
никто не считает своей задачей, и в ТЗ (п.10.2) она, вероятно, не прописана явно. Обратите
внимание: в `EditBarcode` `userInfo` берётся и передаётся дальше — то есть привычка есть,
но в read-пути её забыли. Отсутствие проверки владельца в сервисе, который производит
реплики удостоверений личности, — это не только техническая, но и юридическая проблема
(GDPR/CCPA: несанкционированный доступ к персональным данным).

---

## E-02. IDOR: редактирование чужого баркода и сжигание чужой free-edit квоты

### Суть
`POST /api/v1/barcode/:id/edit` получает `userID` вызывающего, но использует его только
для `billing.Block`. Владелец редактируемой записи не проверяется, поэтому злоумышленник может
изменить документ другого пользователя и израсходовать его единственное право бесплатной правки.

### Место
- `internal/usecase/edit_usecase.go:56-176` — `EditUseCase.Execute`
- `internal/usecase/edit_usecase.go:98-101` — `u.history.GetBarcode(ctx, barcodeID)` без `userID`
- `internal/usecase/edit_usecase.go:110-118` — `billing.Block` с `userID` **вызывающего**
- `internal/transport/http/gin/api_handler.go:96-121` — `APIHandler.EditBarcode`

### Подробное описание

`userID` доходит до usecase корректно:

```go
userInfo, _ := GetUserInfo(c)
req.IdempotencyKey = c.GetHeader("X-Idempotency-Key")
result, err := h.edit.Execute(c.Request.Context(), userInfo.UserID, barcodeID, req)
```

Внутри `Execute` он используется ровно один раз:

```go
sagaID := "edit-" + barcodeID
if err := u.billing.Block(ctx, domain.BlockRequest{
	UserID: userID,      // ← единственное использование
	Units:  0,
	SagaID: sagaID,
}); err != nil {
```

А запись достаётся без привязки к нему:

```go
canEdit, err := u.history.CheckFreeEdit(ctx, barcodeID)     // строка 84 — только по barcodeID
...
record, err := u.history.GetBarcode(ctx, barcodeID)          // строка 98 — только по barcodeID
```

Последствия атаки `attacker` → `barcodeID` жертвы:

1. `CheckFreeEdit(barcodeID)` — читает флаг **жертвы**; если она правкой не пользовалась, вернёт `true`.
2. `GetBarcode(barcodeID)` — отдаёт поля жертвы (тот же дефект, что E-01, но уже в usecase).
3. `Block{UserID: attacker, Units: 0}` — сага регистрируется на атакующего; `Units: 0`, поэтому
   проверка баланса ничего не отклонит.
4. `GeneratePDF417(fields)` — генерируется документ жертвы с подменённым полем
   (`req.Field`/`req.Value` полностью под контролем атакующего).
5. `PublishBarcodeEdited{ID: barcodeID, NewURL: ...}` — History Service по этому событию
   ставит `editFlag=true` и **обновляет `imageUrl` записи жертвы**.

Итог: документ жертвы подменён на версию, сгенерированную злоумышленником, право бесплатной
правки жертвы израсходовано, восстановить его штатным путём нельзя. Стоимость атаки нулевая
(`Units: 0`), в биллинге она выглядит как обычная бесплатная операция атакующего.

Отдельно отмечу: защита от race condition здесь есть и сделана осознанно
(`u.locker.TryLock(ctx, barcodeID)`, строки 68-82, с подробным комментарием про N параллельных
запросов). То есть автор думал про abuse-сценарии free-edit — но защищал от гонки, а не от
обращения к чужому объекту.

### Трассировка

Трассировка по коду (проба на реальном usecase требует стабов пяти портов; логика однозначна
и читается напрямую):

```
POST /api/v1/barcode/victim-barcode-42/edit
Authorization: Bearer <валидный токен attacker-user>
X-Idempotency-Key: 11111111-1111-1111-1111-111111111111
{"field":"firstName","value":"MALLORY"}

api_handler.go:117  userInfo.UserID = "attacker-user"
                    barcodeID       = "victim-barcode-42"
edit_usecase.go:69  TryLock("victim-barcode-42")          → acquired (лок по объекту, не по юзеру)
edit_usecase.go:84  CheckFreeEdit("victim-barcode-42")    → true  (квота ЖЕРТВЫ)
edit_usecase.go:98  GetBarcode("victim-barcode-42")       → record{UserID:"victim-user", ...}
                    ⚠️ record.UserID != "attacker-user" — НЕ СРАВНИВАЕТСЯ
edit_usecase.go:110 Block{UserID:"attacker-user", Units:0, SagaID:"edit-victim-barcode-42"} → ok
edit_usecase.go:127 GeneratePDF417(fields жертвы + firstName="MALLORY") → newURL
edit_usecase.go:158 PublishBarcodeEdited{ID:"victim-barcode-42", NewURL:newURL}
                    → History: editFlag=true, imageUrl=newURL для записи ЖЕРТВЫ
HTTP 200 {"newUrl":"...","canEdit":true}
```

### Предлагаемый багфикс

Проверять владельца в usecase (не в обработчике — чтобы защита работала и для bulk-путей):

```go
record, err := u.history.GetBarcode(ctx, barcodeID)
if err != nil {
	return domain.EditResponse{}, domain.NewBarcodeGenError(err)
}

// Владелец обязателен: правку может делать только владелец записи.
if record.UserID != userID {
	return domain.EditResponse{}, &domain.AppError{
		Code:       domain.ErrCodeNotFound,
		HTTPStatus: 404,
		Message:    "barcode not found",
	}
}
```

Важно: проверку надо поставить **до** `billing.Block` и **до** `CheckFreeEdit`, иначе
атакующий всё равно сможет считывать состояние чужой квоты (oracle) и создавать мусорные саги.
Порядок в исправленном варианте: `TryLock` → `GetBarcode` → **проверка владельца** →
`CheckFreeEdit` → `Block` → `Generate`.

Это требует переставить текущие шаги 1 и 2, что дополнительно экономит вызов `CheckFreeEdit`
на чужих ID.

### Комментарий

Тот же корень, что у E-01, но последствия хуже: E-01 — чтение, E-02 — запись плюс уничтожение
чужого права. Исправлять оба нужно одним изменением, иначе легко закрыть read-путь и забыть
write-путь. Обратите внимание, что `sagaID := "edit-" + barcodeID` (детерминированный, дефект
B-09) здесь работает как дополнительный усилитель: атакующий может предсказать `sagaID` саги
жертвы и, если Billing дедуплицирует по нему, помешать её легитимной правке.

---

## E-03. Дефолтный `JWT_SECRET` позволяет подделать токен любого пользователя

### Суть
`config.Load()` подставляет `JWT_SECRET = "dev-jwt-secret"`, если переменная не задана.
`LocalValidator` проверяет подпись этим секретом. Секрет захардкожен в публичном исходном коде,
поэтому любой, кто видел репозиторий, может подписать валидный токен с произвольным `userId`
и `role`.

### Место
- `internal/config/config.go:134` — `JWTSecret: getEnv(EnvJWTSecret, "dev-jwt-secret")`
- `internal/adapters/auth/local_validator.go:28-45` — `LocalValidator.ValidateToken`
- `internal/app/api_app.go:104-116` — выбор клиента аутентификации

### Подробное описание

Дефолт задан наравне с остальными:

```go
InternalServiceJWT: getEnv(EnvInternalServiceJWT, "dev-internal-token"),
AdminJWT:           getEnv(EnvAdminJWT, "dev-admin-token"),
JWTSecret:          getEnv(EnvJWTSecret, "dev-jwt-secret"),
```

`LocalValidator` — HMAC-валидатор на shared secret:

```go
parsed, err := jwt.Parse(token, func(t *jwt.Token) (interface{}, error) {
	if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
		return nil, fmt.Errorf("auth: unexpected signing method: %v", t.Header["alg"])
	}
	return v.secret, nil
})
```

HMAC симметричен: тот же секрет, которым проверяют, позволяет подписывать. Утечка секрета
эквивалентна утечке права выпускать токены за Auth Service. И `userID`, и `role`, и `permissions`
берутся из claims без дополнительной верификации:

```go
userID, _ := claims["userId"].(string)
...
role, _ := claims["role"].(string)
```

Совмещение с E-01/E-02 даёт полную цепочку: подделать токен → прочитать любой баркод →
отредактировать любой баркод. Причём генерация за чужой счёт тоже возможна: `Generate`
передаёт `userInfo.UserID` в `Block`/`Capture`, то есть списание пойдёт с кошелька
пользователя, чей `userId` подставлен в токен.

Механизм `getEnv` усугубляет проблему: пустая строка трактуется как «не задано» и молча
заменяется дефолтом. Опечатка в имени переменной (`JWT_SECERT`) или пустое значение
в secret-manager не приводят ни к ошибке, ни к предупреждению — сервис поднимается
с `dev-jwt-secret` и печатает вполне благополучное `auth: using LocalValidator (JWT_SECRET set)`.

Смежная деталь: в коде уже встречается проверка «секрет не дефолтный» через сравнение
с магической строкой (`secret != "dev-jwt-secret"`), то есть проблема осознавалась, но решение
осталось локальным и не влияет на запуск.

### Трассировка

Проба подделывает токен, зная только дефолт из репозитория:

```go
v := NewLocalValidator("dev-jwt-secret")   // сервис поднят без JWT_SECRET

tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
	"userId":      "any-victim-user-id",
	"role":        "admin",
	"permissions": []string{"barcode:generate", "barcode:edit"},
	"exp":         time.Now().Add(time.Hour).Unix(),
})
signed, _ := tok.SignedString([]byte("dev-jwt-secret"))
info, err := v.ValidateToken(context.Background(), signed)
```

Результат:

```
PROBE E-04: forged token → err=<nil> userID="any-victim-user-id" role="admin"
            perms=[barcode:generate barcode:edit]
PROBE E-04 CONFIRMED: подделан токен любого пользователя на дефолтном секрете
```

Токен принят, личность и роль — полностью под контролем атакующего.

### Предлагаемый багфикс

Запретить дефолтные секреты вне явного dev-режима. Ввести `APP_ENV` и strict-валидацию конфига:

```go
// internal/config/config.go

const EnvAppEnv = "APP_ENV"

var devDefaults = map[string]string{
	EnvJWTSecret:          "dev-jwt-secret",
	EnvAdminJWT:           "dev-admin-token",
	EnvInternalServiceJWT: "dev-internal-token",
	EnvBillingInternalKey: "dev-billing-key",
}

// Validate возвращает ошибку, если в production остались dev-дефолты или пустые секреты.
func (c Config) Validate() error {
	if os.Getenv(EnvAppEnv) != "production" {
		return nil
	}
	actual := map[string]string{
		EnvJWTSecret:          c.JWTSecret,
		EnvAdminJWT:           c.AdminJWT,
		EnvInternalServiceJWT: c.InternalServiceJWT,
		EnvBillingInternalKey: c.Services.BillingInternalKey,
	}
	var problems []string
	for key, def := range devDefaults {
		v := actual[key]
		switch {
		case v == "":
			problems = append(problems, key+" is empty")
		case v == def:
			problems = append(problems, key+" uses insecure dev default")
		case len(v) < 32 && key == EnvJWTSecret:
			problems = append(problems, key+" is shorter than 32 bytes")
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("insecure production config: %s", strings.Join(problems, "; "))
	}
	return nil
}
```

И вызывать до сборки приложения:

```go
// cmd/api/main.go
cfg := config.Load()
if err := cfg.Validate(); err != nil {
	log.Fatalf("config: %v", err)   // отказ старта вместо тихой работы на dev-секрете
}
```

Стратегически: HMAC на shared secret между сервисами — слабая схема. Правильнее асимметричная
подпись (RS256/ES256) с публичным ключом через JWKS: BFF получает только публичный ключ и
физически не может выпускать токены. Это отдельная задача, но именно она устраняет класс проблемы.

### Комментарий

Дефолты для локальной разработки — удобная практика, которая становится уязвимостью в момент,
когда «не задано» означает «работать на известном всем секрете» вместо «не стартовать».
Обратите внимание на асимметрию: `getEnvBool`/`getEnvDuration` при некорректном значении
тоже молча берут дефолт — это единая философия конфига в проекте (fail-open), и она же
лежит в основе E-04, E-05, E-06. Одна правка `Validate()` закрывает сразу несколько пунктов.

---

## E-04. Fail-open аутентификация: без ENV поднимается mock, принимающий любой токен

### Суть
Если не заданы ни `JWT_SECRET`, ни `AUTH_URL`, приложение поднимает `auth.MockClient`, который
принимает **любой** непустой Bearer-токен и возвращает фиксированного пользователя `mock-user-id`.
Аутентификация превращается в формальность, а все запросы приписываются одному аккаунту.

### Место
- `internal/app/api_app.go:104-117` — выбор клиента
- `internal/adapters/auth/mock_client.go:21-35` — `MockClient.ValidateToken`

### Подробное описание

Логика выбора:

```go
// MockClient используется только если JWT_SECRET не задан (локальная разработка без токенов).
if secret := cfg.JWTSecret; secret != "" && secret != "dev-jwt-secret" {
	authClient = auth.NewLocalValidator(secret)
	log.Printf("auth: using LocalValidator (JWT_SECRET set)")
} else if url := os.Getenv(config.EnvAuthURL); url != "" {
	authClient = auth.NewHTTPClient(url)
	log.Printf("auth: using HTTP client → %s (legacy fallback)", url)
} else {
	log.Printf("auth: JWT_SECRET not set, using mock client")
	authClient = auth.NewMockClient()
}
```

Здесь есть защита от dev-дефолта (`secret != "dev-jwt-secret"`), но она не отказывает в старте,
а **переводит на менее безопасный путь**: сначала HTTP-фолбэк, затем мок. Единственное следствие
пропущенного `JWT_SECRET` — строка в логе.

Сам мок:

```go
func (c *MockClient) ValidateToken(_ context.Context, token string) (domain.UserInfo, error) {
	if token == "" {
		return domain.UserInfo{}, fmt.Errorf("empty token")
	}
	if strings.HasPrefix(token, "invalid-") {
		return domain.UserInfo{}, fmt.Errorf("token rejected by auth service")
	}
	return domain.UserInfo{
		UserID:      "mock-user-id",
		Email:       "user@example.com",
		Role:        "user",
		Permissions: []string{"barcode:generate", "barcode:edit"},
	}, nil
}
```

`Authorization: Bearer x` достаточно для доступа ко всему `/api/v1`. Отказ происходит только
для токенов с префиксом `invalid-`, что является тестовой конвенцией, а не проверкой.

Отдельный побочный эффект: все пользователи становятся `mock-user-id`, поэтому
списания идут с одного кошелька, а `CheckFreeEdit` в History работает с общей квотой.
Это делает состояние биллинга неразличимым между клиентами (см. также E-05).

Ещё одна деталь: селектор смотрит на `os.Getenv(config.EnvAuthURL)`, тогда как `cfg.Services.AuthURL`
всегда заполнен дефолтом `http://auth-service:3000`. Два источника истины расходятся — тот же
паттерн, что в A-05 с Redis: конфиг говорит «URL есть», ветвление считает, что его нет.

### Трассировка

```
Запуск: docker run bff-api            # ENV не переданы
api_app.go:115   log: "auth: JWT_SECRET not set, using mock client"
                 authClient = auth.MockClient{}

Запрос:  GET /api/v1/revisions
         Authorization: Bearer whatever
middleware.go:64 HasPrefix("Bearer whatever", "Bearer ") → true
middleware.go:68 auth.ValidateToken(ctx, "whatever")
mock_client.go:22 token != ""              → продолжаем
mock_client.go:25 !HasPrefix("invalid-")    → продолжаем
mock_client.go:30 return UserInfo{UserID: "mock-user-id", ...}, nil
middleware.go:74 c.Set(userInfo) → доступ разрешён
```

Логи при этом выглядят «нормально»: одна информационная строка при старте, никаких ошибок.

### Предлагаемый багфикс

1. В production запрещать mock-адаптеры вовсе (см. `Validate()` из E-03), расширив проверку:

```go
func (c Config) Validate() error {
	if os.Getenv(EnvAppEnv) != "production" {
		return nil
	}
	var problems []string
	if c.JWTSecret == "" || c.JWTSecret == "dev-jwt-secret" {
		problems = append(problems, "JWT_SECRET must be set to a non-default value")
	}
	for _, req := range []struct{ key, val string }{
		{EnvBillingURL, os.Getenv(EnvBillingURL)},
		{EnvBarcodeGenURL, os.Getenv(EnvBarcodeGenURL)},
		{EnvHistoryURL, os.Getenv(EnvHistoryURL)},
		{EnvKafkaBrokers, os.Getenv(EnvKafkaBrokers)},
		{EnvRedisURL, os.Getenv(EnvRedisURL)},
	} {
		if req.val == "" {
			problems = append(problems, req.key+" must be set in production")
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("insecure production config: %s", strings.Join(problems, "; "))
	}
	return nil
}
```

2. Убрать `MockClient` из production-сборки через build tag, чтобы это гарантировалось
   компилятором, а не дисциплиной:

```go
//go:build !production
// internal/adapters/auth/mock_client.go
```

3. Заменить `log.Printf` на `log.Fatalf` в ветке мока при `APP_ENV=production`.

### Комментарий

Мок-адаптеры сами по себе — сильная сторона проекта: они дают полноценный локальный запуск
без инфраструктуры. Проблема исключительно в **триггере выбора**: «переменная не задана»
и «мы в dev-режиме» — разные условия, а код их отождествляет. Явный `APP_ENV` разделяет их
и сохраняет удобство разработки. Смежные пункты: A-05 (Redis), E-06 (Billing) — один и тот же
паттерн в четырёх местах.

---

## E-05. `ENABLE_LEGACY_AUTH=false` схлопывает всех пользователей в одного `anonymous`

### Суть
Флаг `ENABLE_LEGACY_AUTH=false` полностью отключает проверку токена и подставляет
`UserID: "anonymous"` всем запросам. Это не только снимает аутентификацию, но и объединяет
биллинг, историю и квоту бесплатных правок всех клиентов в один аккаунт.

### Место
- `internal/transport/http/gin/middleware.go:50-58` — `UserJWTMiddleware`
- `internal/config/config.go:170` — `EnableLegacyAuth: getEnvBool(EnvEnableLegacyAuth, true)`

### Подробное описание

```go
func UserJWTMiddleware(auth ports.AuthClient, enableLegacyAuth bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !enableLegacyAuth {
			// Feature flag отключён — пропускаем аутентификацию
			c.Set(ContextKeyUserInfo, domain.UserInfo{UserID: "anonymous"})
			c.Next()
			return
		}
		...
	}
}
```

Заголовок `Authorization` не проверяется вообще — можно его не присылать. Все обработчики
получают `userInfo.UserID == "anonymous"`, и это значение уходит:

- в `billing.Block/Capture/Release` → списания с общего «кошелька anonymous»;
- в `history` → все баркоды принадлежат `anonymous`;
- в события `barcode.generated` / `trans-history.log` → аудит теряет привязку к клиенту.

Совместно с E-01/E-02 наступает вырожденный случай: проверка владельца, даже если её добавить,
станет бессмысленной — все записи принадлежат одному «пользователю», и любой клиент
законно проходит проверку `record.UserID == userInfo.UserID`.

Дефолт флага — `true` (безопасный), и это правильно. Риск в том, что флаг задуман как
rollback-механизм (п.15 ТЗ): в инциденте его могут выставить в `false`, чтобы «обойти
недоступный Auth Service», не осознавая, что это открывает API всему интернету и перемешивает
биллинг. `getEnvBool` при некорректном значении молча берёт дефолт, так что опечатки
безопасны — но осознанное `false` ничем не ограничено.

### Трассировка

```
ENV: ENABLE_LEGACY_AUTH=false
config.go:170     Features.EnableLegacyAuth = false
router.go:41      api.Use(UserJWTMiddleware(auth, false))

Запрос: POST /api/v1/barcode/generate     (без Authorization вообще)
        X-Idempotency-Key: 22222222-2222-2222-2222-222222222222
        {"revision":"US_CA_08292017","units":5,"fields":{...}}

middleware.go:52  !enableLegacyAuth → true
middleware.go:54  c.Set(userInfo, UserInfo{UserID: "anonymous"})
middleware.go:55  c.Next()          ← токен не запрашивался
api_handler.go:79 userInfo.UserID = "anonymous"
generate_usecase   quote/Block/Capture  для userID="anonymous"
→ списание с общего аккаунта; в History запись с userId="anonymous"
```

### Предлагаемый багфикс

Запретить отключение аутентификации в production и сделать эффект флага явным в логах:

```go
// internal/config/config.go — в Validate()
if os.Getenv(EnvAppEnv) == "production" && !c.Features.EnableLegacyAuth {
	problems = append(problems, "ENABLE_LEGACY_AUTH=false is forbidden in production")
}
```

Если сценарий «работать при недоступном Auth Service» действительно нужен, он должен
сохранять личность пользователя, а не обнулять её — например, локальная валидация подписи
без обращения к Auth (что уже умеет `LocalValidator`), с деградацией только в части
обогащения профиля:

```go
if !enableLegacyAuth {
	// Degraded mode: проверяем подпись локально, но не обращаемся к Auth Service
	// за профилем. Личность пользователя сохраняется.
	header := c.GetHeader("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing bearer token"})
		return
	}
	info, err := localOnly.ValidateToken(c.Request.Context(), strings.TrimPrefix(header, "Bearer "))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired token"})
		return
	}
	c.Set(ContextKeyUserInfo, info)
	c.Next()
	return
}
```

Плюс метрика/алерт `bff_auth_disabled_mode 1`, чтобы состояние было видно в мониторинге,
а не только в строке лога при старте.

### Комментарий

Флаг решает реальную задачу (rollback по п.15 ТЗ), но выбранная деградация — «пускать всех
как одного» — меняет модель безопасности и модель биллинга одновременно. Правильная деградация
для BFF: перестать обогащать профиль, но продолжать проверять подпись. Совместно с E-03 и E-04
это третий способ получить одно и то же состояние «аутентификации фактически нет», причём все
три включаются не кодом, а конфигурацией.

---

## E-06. Fail-open биллинг: без `BILLING_URL` поднимается mock — бесплатная генерация

### Суть
Если `BILLING_URL` не задана, поднимается `billing.MockClient`, который подтверждает любые
котировки и блокировки. Платная генерация становится бесплатной, при этом сервис отвечает
`200 OK` и выдаёт реальные баркоды (BarcodeGen может быть настроен по-настоящему).

### Место
- `internal/app/api_app.go:119-128` — выбор клиента биллинга
- `internal/adapters/billing/mock_client.go` — `MockClient`
- `internal/config/config.go:139` — дефолт `BillingURL: "http://billing:3000"` (не используется в селекторе)

### Подробное описание

```go
// Billing: HTTPClient если BILLING_URL задан явно, иначе Mock (п.7 ТЗ).
if url := os.Getenv(config.EnvBillingURL); url != "" {
	billingClient = billing.NewHTTPClient(url, cfg.Timeouts.Billing, cfg.Services.BillingInternalKey)
	log.Printf("billing: using HTTP client → %s", url)
} else {
	log.Printf("billing: BILLING_URL not set, using mock client")
	billingClient = billing.NewMockClient(cfg.UnitPrice)
}
```

Опасная комбинация — частичная конфигурация: `BARCODEGEN_URL` задан, `BILLING_URL` забыт.
Тогда BarcodeGen настоящий (артефакты реальные), а биллинг фиктивный (списаний нет).
Сервис работает без ошибок и раздаёт платный продукт бесплатно. Никакого сигнала, кроме
информационной строки в логе при старте.

Здесь же проявляется расхождение источников истины: `cfg.Services.BillingURL` всегда содержит
`http://billing:3000`, но селектор смотрит на `os.Getenv`. То есть конфигурация утверждает,
что адрес известен, а сборка приложения решает, что биллинга нет. Идентично E-04 (Auth)
и A-05 (Redis).

Смежный дефект: `BILLING_INTERNAL_API_KEY` имеет дефолт `dev-billing-key` (см. E-17), поэтому
даже при заданном `BILLING_URL` вызовы могут уходить с dev-ключом.

### Трассировка

```
ENV: BARCODEGEN_URL=http://barcodegen:8080   (реальный)
     BILLING_URL не задан

api_app.go:126  log: "billing: BILLING_URL not set, using mock client"
                billingClient = billing.MockClient{unitPrice: 0.50}
api_app.go:133  log: "barcodegen: using HTTP client → http://barcodegen:8080"

POST /api/v1/barcode/generate {"units":100,...}
generate_usecase  Quote(units=100)  → mock: amount=50.00, sufficient=true
generate_usecase  Block(units=100)  → mock: nil (ничего не заблокировано)
generate_usecase  Generate ×100     → РЕАЛЬНЫЙ BarcodeGen, 100 артефактов
generate_usecase  Capture(...)      → mock: nil (ничего не списано)
HTTP 200 + 100 реальных баркодов, оплата: 0
```

### Предлагаемый багфикс

Тот же strict-режим (см. E-03/E-04): в `APP_ENV=production` отсутствие `BILLING_URL` —
фатальная ошибка старта. Дополнительно — экспортировать текущий режим адаптеров в метрику,
чтобы «мок в проде» был виден в мониторинге:

```go
// internal/metrics/metrics.go
// AdapterMode — 1 если адаптер работает в mock-режиме, 0 если реальный.
// Позволяет поставить алерт: bff_adapter_mock_mode{adapter="billing"} == 1.
var AdapterMode = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "bff_adapter_mock_mode",
		Help: "1 if adapter runs in mock mode, 0 if real client is used.",
	},
	[]string{"adapter"},
)
```

```go
// internal/app/api_app.go
if url := os.Getenv(config.EnvBillingURL); url != "" {
	billingClient = billing.NewHTTPClient(url, cfg.Timeouts.Billing, cfg.Services.BillingInternalKey)
	metrics.AdapterMode.WithLabelValues("billing").Set(0)
} else {
	billingClient = billing.NewMockClient(cfg.UnitPrice)
	metrics.AdapterMode.WithLabelValues("billing").Set(1)
}
```

И убрать неиспользуемые дефолты URL из `config.Load()` либо, наоборот, использовать
`cfg.Services.*` в селекторах — но не держать оба варианта одновременно.

### Комментарий

Это тот же дефект, что был отмечен в обзоре кодовой базы, но здесь он трассирован в связке
с E-17 и с расхождением `cfg` vs `os.Getenv`. Экономический риск прямой: сервис отдаёт платный
продукт без оплаты, и обнаружить это можно только по расхождению отчётов, а не по ошибкам.
Одна метрика `bff_adapter_mock_mode` закрывает наблюдаемость для всех пяти адаптеров сразу.

---

## E-07. `/admin` и `/internal` защищены статическим shared secret, а не JWT

### Суть
`AdminJWTMiddleware` и `ServiceJWTMiddleware`, несмотря на названия, не разбирают JWT.
Они сравнивают Bearer-строку со статическим значением из ENV. Нет срока действия, ротации,
идентификации вызывающего и проверки `role=admin`, обещанной в комментарии к роутеру.

### Место
- `internal/transport/http/gin/middleware.go:90-105` — `ServiceJWTMiddleware`
- `internal/transport/http/gin/middleware.go:107-124` — `AdminJWTMiddleware`
- `internal/transport/http/gin/router.go:18-21` — комментарий «/admin — Admin JWT (role=admin)»

### Подробное описание

```go
func AdminJWTMiddleware(expectedToken string) gin.HandlerFunc {
	expected := []byte(expectedToken)
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		if !strings.HasPrefix(header, "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing bearer token"})
			return
		}
		token := strings.TrimPrefix(header, "Bearer ")
		if subtle.ConstantTimeCompare([]byte(token), expected) != 1 {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "invalid admin token"})
			return
		}
		c.Next()
	}
}
```

Что сделано правильно: `subtle.ConstantTimeCompare` действительно защищает от timing-атаки
(и это отмечено в комментарии со ссылкой на предыдущий отчёт). Сравнение строгое — проба
подтвердила, что ни префикс, ни другой регистр не проходят.

Чего нет:

| Свойство | Статус |
|----------|--------|
| Срок действия | ❌ токен вечен |
| Ротация без простоя | ❌ одно значение в ENV, смена = рестарт всех реплик |
| Идентификация вызывающего | ❌ в логах и аудите не видно, *кто* сделал admin-действие |
| Проверка `role=admin` | ❌ роли нет как понятия |
| Отзыв конкретного клиента | ❌ отзывается только глобально |
| Разграничение прав внутри `/admin` | ❌ один токен = все админ-операции |

Что доступно с этим токеном: `PUT /admin/revisions/:revision` (изменение цепочки вычислений
всех генераций), `PUT /admin/config/timeouts`, `PUT /admin/config/topup-bonus` (проценты
бонусов при пополнении — прямое влияние на деньги). Дефолтное значение — `dev-admin-token`
(см. E-03), то есть при незаданной `ADMIN_JWT` доступ имеет любой читатель репозитория.

Смежная деталь по аудиту: ни один admin-обработчик не пишет запись «кто и что изменил»
(`admin_handler.go` целиком без логирования). При shared secret установить автора изменения
невозможно даже теоретически.

### Трассировка

Проба на реальном middleware:

```
PROBE E-01b: token="dev-admin-token"      → status=200
PROBE E-01b: token="dev-admin-token-extra" → status=403
PROBE E-01b: token="dev-admin"            → status=403
PROBE E-01b: token="DEV-ADMIN-TOKEN"      → status=403
```

Сравнение строгое и корректное — уязвимость не в сравнении, а в самой модели «один вечный
статический секрет».

### Предлагаемый багфикс

Краткосрочно (минимальное изменение, закрывает аудит и утечку по логам):

```go
// AdminJWTMiddleware проверяет admin-доступ и фиксирует вызывающего для аудита.
func AdminJWTMiddleware(expectedToken string) gin.HandlerFunc {
	expected := []byte(expectedToken)
	if len(expected) == 0 {
		panic("gintransport: admin token must not be empty") // см. E-08
	}
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		if !strings.HasPrefix(header, "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing bearer token"})
			return
		}
		token := strings.TrimPrefix(header, "Bearer ")
		if subtle.ConstantTimeCompare([]byte(token), expected) != 1 {
			log.Printf("audit: admin auth failed path=%s ip=%s", c.FullPath(), c.ClientIP())
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "invalid admin token"})
			return
		}
		log.Printf("audit: admin access path=%s method=%s ip=%s",
			c.FullPath(), c.Request.Method, c.ClientIP())
		c.Next()
	}
}
```

Правильно (то, что обещает комментарий в роутере): валидировать реальный JWT и требовать роль.
Переиспользуя существующий `LocalValidator`:

```go
func AdminJWTMiddleware(auth ports.AuthClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		if !strings.HasPrefix(header, "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing bearer token"})
			return
		}
		info, err := auth.ValidateToken(c.Request.Context(), strings.TrimPrefix(header, "Bearer "))
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired token"})
			return
		}
		if info.Role != "admin" {
			log.Printf("audit: admin denied user=%s role=%s path=%s", info.UserID, info.Role, c.FullPath())
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "admin role required"})
			return
		}
		c.Set(ContextKeyUserInfo, info)
		log.Printf("audit: admin action user=%s path=%s method=%s",
			info.UserID, c.FullPath(), c.Request.Method)
		c.Next()
	}
}
```

Это даёт срок действия, отзыв, конкретного автора в аудите и приводит код в соответствие
с собственной документацией.

### Комментарий

Расхождение «название говорит JWT — код делает shared secret» опаснее, чем сам shared secret:
читающий роутер видит комментарий `/admin — Admin JWT (role=admin)` и делает вывод, что защита
ролевая. При инвентаризации безопасности такие места systematически пропускают. Как минимум
нужно переименовать (`AdminTokenMiddleware`) и поправить комментарий, даже если полноценный
JWT отложен.

---

## E-08. Пустой `expectedToken` открывает `/admin` и `/internal` полностью

### Суть
При `expectedToken == ""` сравнение `subtle.ConstantTimeCompare([]byte(""), []byte(""))`
возвращает `1`, поэтому запрос с заголовком ровно `Authorization: Bearer ` (без токена)
проходит проверку. Достаточно, чтобы `ADMIN_JWT` оказался пустым в одной точке сборки.

### Место
- `internal/transport/http/gin/middleware.go:107-124` — `AdminJWTMiddleware`
- `internal/transport/http/gin/middleware.go:90-105` — `ServiceJWTMiddleware` (идентично)

### Подробное описание

`subtle.ConstantTimeCompare` по контракту возвращает `1`, когда срезы равны по длине
и содержимому; для двух пустых срезов это выполняется. Поэтому пустой ожидаемый токен
означает «пустой присланный токен верен».

Текущая конфигурация от этого частично защищает: `getEnv` трактует пустую строку как
«не задано» и подставляет `dev-admin-token`:

```go
func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
```

То есть через `config.Load()` пустое значение недостижимо — дефект **латентный**. Но он
реализуется, если:

- middleware вызывается из другой точки сборки (тесты, будущий вариант роутера, отдельный
  admin-бинарь), где токен берётся не из `config.Load()`;
- конфиг переводят на другой источник (Vault/Consul/файл), где пустая строка — валидное значение;
- кто-то «оптимизирует» `getEnv`, убрав проверку `value != ""` (например, чтобы различать
  «не задано» и «задано пустым»).

Стоимость страховки — одна проверка при инициализации, поэтому опираться на инвариант
из другого пакета здесь неоправданно.

### Трассировка

Проба на реальном middleware с `expectedToken == ""`:

```go
r.Use(AdminJWTMiddleware(""))
r.GET("/admin/ping", func(c *gin.Context) { c.String(http.StatusOK, "admin-area") })

req.Header.Set("Authorization", "Bearer ")   // ровно "Bearer " без токена
```

Результат:

```
PROBE E-01: expected="" header="Bearer " → status=200 body="admin-area"
PROBE E-01 CONFIRMED: доступ к /admin получен без токена
```

Пошагово: `HasPrefix("Bearer ", "Bearer ")` → `true`;
`TrimPrefix` → `""`; `ConstantTimeCompare([]byte(""), []byte(""))` → `1` → `c.Next()`.

### Предлагаемый багфикс

Отказ на этапе инициализации — ошибка конфигурации должна проявляться при старте, а не в рантайме:

```go
func AdminJWTMiddleware(expectedToken string) gin.HandlerFunc {
	if strings.TrimSpace(expectedToken) == "" {
		// Пустой ожидаемый токен означает, что ConstantTimeCompare("", "") == 1,
		// то есть /admin открыт для запроса с заголовком "Bearer ".
		panic("gintransport: admin token must not be empty")
	}
	expected := []byte(expectedToken)
	...
}
```

То же для `ServiceJWTMiddleware`. Дополнительно — отвергать пустой токен в самом обработчике,
чтобы защита не зависела от инициализации:

```go
token := strings.TrimPrefix(header, "Bearer ")
if token == "" {
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing bearer token"})
	return
}
```

Тест-регрессия: `AdminJWTMiddleware("")` должен паниковать; `Bearer ` при непустом ожидаемом
токене — `401`.

### Комментарий

Классическая ловушка constant-time-сравнения: функция выбрана правильно (защита от timing),
но её граничное поведение на пустом входе противоположно интуиции — «ничего не совпадает
с ничем» на самом деле совпадает. Сейчас спасает `getEnv`, то есть безопасность middleware
держится на деталях реализации соседнего пакета. Две строки проверки убирают эту зависимость.

---

## E-09. JWT без claim `exp` принимается бессрочно; нет проверки `iss`/`aud`

### Суть
`LocalValidator` не требует наличия `exp`. Токен, выпущенный без срока действия, валиден
неограниченно долго, отозвать его невозможно. Также не проверяются `iss` (издатель)
и `aud` (аудитория), поэтому подойдёт токен, выпущенный для другого сервиса на том же секрете.

### Место
- `internal/adapters/auth/local_validator.go:28-45` — `ValidateToken`

### Подробное описание

```go
parsed, err := jwt.Parse(token, func(t *jwt.Token) (interface{}, error) {
	if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
		return nil, fmt.Errorf("auth: unexpected signing method: %v", t.Header["alg"])
	}
	return v.secret, nil
})
```

`golang-jwt/v5` проверяет `exp`, **если claim присутствует**; при отсутствии — считает токен
валидным. Опции `jwt.WithExpirationRequired()`, `jwt.WithIssuer()`, `jwt.WithAudience()`
не переданы.

Последствия:

1. **Бессрочные токены.** Утёкший токен без `exp` действует до смены `JWT_SECRET`, то есть
   до перезапуска всех реплик с новым секретом. Механизма отзыва (blacklist, `jti`) нет.
2. **Отсутствие изоляции по аудитории.** Все сервисы, использующие тот же `JWT_SECRET`
   (по комментарию в `config.go` — Auth Service и BFF; фактически столько, сколько получили
   секрет), принимают токены друг друга. Токен, выданный для одного сервиса, годится для BFF.
3. **Нет `iss`.** Невозможно отличить токен Auth Service от токена любого другого владельца
   секрета.

Что сделано правильно (и подтверждено пробой E-20): проверка алгоритма. `alg=none` отклоняется,
подмена HMAC на RSA невозможна — keyfunc принимает только `*jwt.SigningMethodHMAC`. Это
закрывает классическую alg-confusion.

### Трассировка

Токен без `exp`:

```
PROBE E-02: token without exp → err=<nil> userID="user-1"
PROBE E-02 CONFIRMED: бессрочный токен принят, отозвать его невозможно
```

Контроль — истёкший токен отклоняется корректно:

```
PROBE E-02b: expired token → err=auth: invalid token: token has invalid claims: token is expired
```

Контроль — `alg=none` отклоняется:

```
PROBE E-05: alg=none → err=auth: invalid token: token is unverifiable:
            error while executing keyfunc: auth: unexpected signing method: none
```

То есть валидатор строг к алгоритму и к истёкшему `exp`, но безразличен к его отсутствию.

### Предлагаемый багфикс

```go
type LocalValidator struct {
	secret   []byte
	issuer   string
	audience string
}

func NewLocalValidator(secret, issuer, audience string) *LocalValidator {
	return &LocalValidator{secret: []byte(secret), issuer: issuer, audience: audience}
}

func (v *LocalValidator) ValidateToken(_ context.Context, token string) (domain.UserInfo, error) {
	opts := []jwt.ParserOption{
		jwt.WithValidMethods([]string{"HS256", "HS384", "HS512"}),
		jwt.WithExpirationRequired(),          // exp обязателен
		jwt.WithLeeway(30 * time.Second),      // допуск на расхождение часов
	}
	if v.issuer != "" {
		opts = append(opts, jwt.WithIssuer(v.issuer))
	}
	if v.audience != "" {
		opts = append(opts, jwt.WithAudience(v.audience))
	}

	parsed, err := jwt.Parse(token, func(t *jwt.Token) (interface{}, error) {
		return v.secret, nil          // проверку alg делает WithValidMethods
	}, opts...)
	if err != nil {
		return domain.UserInfo{}, fmt.Errorf("auth: invalid token: %w", err)
	}
	...
}
```

`jwt.WithValidMethods` заменяет ручную проверку метода — то же свойство, но декларативно
и без риска ошибиться при рефакторинге keyfunc. Значения `iss`/`aud` вынести в ENV
(`JWT_ISSUER`, `JWT_AUDIENCE`) и согласовать с Auth Service.

Дополнительно стоит ограничить максимальный TTL: если Auth Service выпускает токены на месяцы,
`exp` формально есть, но практической разницы с бессрочным мало.

### Комментарий

Дефект относится к классу «валидация по умолчанию не значит валидация»: библиотека проверяет
только те claims, которые ей дали, и умолчания у неё разумно-либеральные. Учитывая E-03
(подделываемый секрет), это второй по значимости пункт в блоке аутентификации: даже после
исправления секрета отсутствие `exp` оставляет невозможность отзыва.

---

## E-10. Один секрет `INTERNAL_SERVICE_JWT` защищает `/internal/*` и `/api/v1/bulk/wake`

### Суть
`ServiceJWTMiddleware(internalJWT)` применяется и к группе `/internal`, и к пользовательскому
по расположению маршруту `/api/v1/bulk/wake`. Компрометация секрета одного вызывающего
(Bulk Service) открывает все внутренние capability-эндпоинты, включая генерацию в обход биллинга.

### Место
- `internal/transport/http/gin/router.go:84-89` — `bulkWake` группа
- `internal/transport/http/gin/router.go:113-129` — `internal` группа
- `internal/transport/http/gin/router.go:120-128` — состав `/internal`

### Подробное описание

Один и тот же параметр `internalJWT` используется дважды:

```go
bulkWake := r.Group("/api/v1/bulk")
bulkWake.Use(ServiceJWTMiddleware(internalJWT))
{
	bulkWake.POST("/wake", h.API.BulkWake)
}
...
internal := r.Group("/internal")
internal.Use(ServiceJWTMiddleware(internalJWT))
{
	internal.POST("/validate", h.Internal.Validate)
	internal.POST("/billing/quote", h.Internal.Quote)
	internal.POST("/billing/block-batch", ..., h.Internal.BlockBatch)
	internal.GET("/revisions", h.Internal.ListRevisions)
	internal.GET("/revisions/:revision/schema", h.Internal.GetRevisionSchema)
	internal.POST("/v1/capabilities/generate-raw", h.Internal.GenerateRaw)
}
```

Вызывающие разные: `/api/v1/bulk/wake` дергает Bulk Service, `/internal/*` — Verification Service
и другие внутренние потребители. Секрет общий, поэтому:

- утечка из Bulk Service даёт доступ к `POST /internal/v1/capabilities/generate-raw`
  (генерация артефактов) и `POST /internal/billing/block-batch` (блокировка средств);
- ротация секрета требует одновременного обновления всех сервисов-клиентов;
- по факту обращения невозможно определить, какой сервис его сделал (нет `sub`/`client_id`).

Дополнительно: `/api/v1/bulk/wake` находится в префиксе `/api/v1`, но защищён service-токеном.
Порядок регистрации групп в Gin делает это работоспособным (отдельная группа `/api/v1/bulk`
со своим middleware), однако для читающего роутер правило «`/api/v1` = User JWT» здесь нарушено —
источник ошибок при добавлении новых маршрутов в `/api/v1/bulk/...`, которые неожиданно
окажутся под service-токеном вместо пользовательского.

### Трассировка

```
ENV: INTERNAL_SERVICE_JWT=<секрет S>
     (S роздан Bulk Service и Verification Service)

Компрометация: S утёк из логов/конфига Bulk Service.

Запрос 1: POST /api/v1/bulk/wake            Bearer S → 200 (ожидаемое использование)
Запрос 2: POST /internal/v1/capabilities/generate-raw  Bearer S → 200
          ← та же строка даёт доступ к capability-генерации
Запрос 3: POST /internal/billing/block-batch          Bearer S → 200
          ← и к пакетной блокировке средств
```

### Предлагаемый багфикс

Разделить секреты по вызывающим:

```go
// internal/config/config.go
EnvBulkServiceJWT = "BULK_SERVICE_JWT"
...
BulkServiceJWT: getEnv(EnvBulkServiceJWT, "dev-bulk-token"),
```

```go
// internal/transport/http/gin/router.go
bulkWake := r.Group("/api/v1/bulk")
bulkWake.Use(ServiceJWTMiddleware(bulkJWT))     // отдельный секрет
```

Правильнее — сделать вызывающего идентифицируемым: подписанный service JWT с claim `sub`
(имя сервиса) и списком разрешённых capability, тогда одна проверка покрывает и аутентификацию,
и авторизацию:

```go
func ServiceAuthMiddleware(auth ports.AuthClient, requiredScope string) gin.HandlerFunc {
	return func(c *gin.Context) {
		info, err := auth.ValidateToken(c.Request.Context(),
			strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer "))
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid service token"})
			return
		}
		if !slices.Contains(info.Permissions, requiredScope) {
			log.Printf("audit: service %s denied scope=%s path=%s", info.UserID, requiredScope, c.FullPath())
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "insufficient scope"})
			return
		}
		c.Next()
	}
}
```

Тогда `/api/v1/bulk/wake` требует `scope=bulk:wake`, а `generate-raw` — `scope=capability:generate`,
и утечка токена Bulk Service не даёт доступа к генерации.

### Комментарий

Совмещено с E-07: пока внутренняя аутентификация построена на статических строках, любое
разделение прав сводится к «сколько разных строк мы завели». Минимальный шаг — второй секрет
для Bulk. Полное решение — scopes, и оно же убирает необходимость плодить ENV-переменные
при появлении новых внутренних клиентов.

---

## E-11. Нет лимита размера тела запроса

### Суть
Ни `http.MaxBytesReader`, ни лимит на уровне Gin/сервера не установлены. Любой аутентифицированный
клиент может отправить произвольно большой JSON, который будет полностью прочитан в память
и разобран. Проба: 8 МБ тело обработано с `200 OK`.

### Место
- `internal/transport/http/gin/router.go:31-34` — сборка движка, лимитов нет
- `internal/transport/http/gin/server.go` — `http.Server` без ограничений
- `internal/transport/http/gin/idempotency_middleware.go` — лимит есть только на **ответ** (1 МБ)

### Подробное описание

В `NewRouter` подключены `MaintenanceModeMiddleware`, `MetricsMiddleware`, `gin.Recovery()` —
ограничения размера запроса среди них нет. `c.ShouldBindJSON` читает тело целиком.

Усиливающие факторы:

1. `GenerateRequest.Fields` — `map[string]any` без ограничений на число ключей, длину значений
   и глубину вложенности. Валидация полей идёт **после** разбора JSON.
2Ы. `units` в `GenerateRequest` — при большом значении раздувается число вызовов BarcodeGen
   (пересекается с логикой раздела C).
3. Идемпотентность кэширует **ответы** с лимитом 1 МБ (`maxIdempotencyBodySize`), то есть
   про лимиты в проекте думали — но только на стороне ответа.
4. Отсутствие rate limiting (E-12) позволяет отправлять такие запросы параллельно и непрерывно.

Оценка воздействия: N параллельных запросов по M МБ дают N×M МБ аллокаций одновременно;
при отсутствии `MemoryLimit`/`GOMEMLIMIT` это ведёт к OOM-kill контейнера. Поскольку
in-memory сторы (идемпотентность при отсутствии Redis — A-05) живут в том же процессе,
рестарт стирает и их состояние, что открывает окно для повторной оплаты (B-06).

### Трассировка

```go
huge := `{"revision":"US_CA_08292017","fields":{"x":"` + strings.Repeat("A", 8*1024*1024) + `"}}`
req := httptest.NewRequest(http.MethodPost, "/api/v1/barcode/generate", strings.NewReader(huge))
```

Результат:

```
PROBE E-06: body=8388655 bytes → status=200 resp="{\"fieldsSeen\":2}"
PROBE E-06 CONFIRMED: 8MB тело прочитано и разобрано без лимита
```

Тело принято, разобрано, отказа по размеру нет ни на одном уровне.

### Предлагаемый багфикс

Middleware с лимитом, применяемый глобально:

```go
// internal/transport/http/gin/middleware.go

// MaxBodySizeMiddleware ограничивает размер тела запроса.
// Возвращает 413 до полного чтения тела в память.
func MaxBodySizeMiddleware(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.ContentLength > maxBytes {
			c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{
				"error": "request body too large",
				"code":  "PAYLOAD_TOO_LARGE",
			})
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
		c.Next()
	}
}
```

```go
// internal/transport/http/gin/router.go
const maxRequestBodyBytes = 1 << 20 // 1 МБ — согласовано с лимитом кэша ответов

r := gin.New()
r.Use(MaintenanceModeMiddleware(maintenanceMode))
r.Use(MaxBodySizeMiddleware(maxRequestBodyBytes))
r.Use(MetricsMiddleware())
r.Use(gin.Recovery())
```

Проверка `ContentLength` отсекает честных клиентов дешево, `MaxBytesReader` защищает
от `Transfer-Encoding: chunked` без `Content-Length`. Дополнительно — ограничить
`len(req.Fields)` и `units` в доменной валидации, а также выставить `GOMEMLIMIT` в контейнере.

### Комментарий

Асимметрия показательна: лимит на ответ (1 МБ в идемпотентности) есть, на запрос — нет.
Вероятно потому, что первый был добавлен в ответ на конкретный инцидент с OOM, а входящий
трафик по умолчанию считается доверенным. С учётом E-03/E-04 (обход аутентификации)
«доверенный клиент» — неверная посылка.

---

## E-12. Нет rate limiting

### Суть
Ни один эндпоинт не ограничен по частоте запросов. Это открывает перебор `barcodeID`
(усиливает E-01), брутфорс admin/service-токенов (E-07), исчерпание квот downstream-сервисов
и денежные abuse-сценарии.

### Место
- `internal/transport/http/gin/router.go` — весь роутер, middleware отсутствует
- поиск по `rate|limiter` в `internal/` — совпадений в production-коде нет

### Подробное описание

Отсутствие лимитов пересекается со всеми остальными пунктами раздела:

| Сценарий | Что усиливается |
|----------|-----------------|
| Перебор `GET /api/v1/barcode/:id` | E-01 — массовая выкачка PII |
| Брутфорс `Bearer` на `/admin` | E-07 — токен статичен и вечен, время на перебор не ограничено |
| Поток `POST /barcode/generate` | Исчерпание AI/BarcodeGen квот, рост расходов |
| Большие тела (E-11) | OOM за счёт параллелизма |
| Поток `POST /barcode/:id/edit` | Нагрузка на Redis-локи и History |

Особенно важен второй пункт: статический вечный токен + отсутствие лимита попыток = перебор
ограничен только пропускной способностью сети. Для JWT с коротким `exp` это менее критично,
здесь — критично.

### Трассировка

Трассировка по конфигурации роутера: цепочка middleware для `/api/v1` состоит из
`MaintenanceModeMiddleware` → `MetricsMiddleware` → `gin.Recovery()` → `UserJWTMiddleware`
→ (`idempotencyKeyRequired` → `IdempotencyMiddleware` для write-операций) → handler.
Ни один из них не ведёт счёт запросов на клиента. `MetricsMiddleware` только инкрементирует
счётчик Prometheus и ничего не отклоняет.

### Предлагаемый багфикс

Лимит по идентичности (userID из JWT), с фолбэком на IP для неаутентифицированных путей.
Для одной реплики достаточно `golang.org/x/time/rate`:

```go
// internal/transport/http/gin/ratelimit.go

type rateLimiter struct {
	mu       sync.Mutex
	visitors map[string]*rate.Limiter
	rps      rate.Limit
	burst    int
}

func NewRateLimiter(rps float64, burst int) *rateLimiter {
	rl := &rateLimiter{visitors: make(map[string]*rate.Limiter), rps: rate.Limit(rps), burst: burst}
	go rl.cleanupLoop()
	return rl
}

func (rl *rateLimiter) limiterFor(key string) *rate.Limiter {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	lim, ok := rl.visitors[key]
	if !ok {
		lim = rate.NewLimiter(rl.rps, rl.burst)
		rl.visitors[key] = lim
	}
	return lim
}

func (rl *rateLimiter) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		key := c.ClientIP()
		if info, ok := GetUserInfo(c); ok && info.UserID != "" {
			key = "user:" + info.UserID    // лимит на пользователя, а не на NAT-адрес
		}
		if !rl.limiterFor(key).Allow() {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": "rate limit exceeded", "code": "RATE_LIMITED",
			})
			return
		}
		c.Next()
	}
}
```

Подключение — после `UserJWTMiddleware`, чтобы ключом был `userID`:

```go
api := r.Group("/api/v1")
api.Use(UserJWTMiddleware(auth, enableLegacyAuth))
api.Use(userRateLimiter.Middleware())        // 10 rps, burst 20
...
admin := r.Group("/admin")
admin.Use(adminRateLimiter.Middleware())     // 1 rps, burst 5 — против брутфорса токена
admin.Use(AdminJWTMiddleware(adminJWT))
```

Для `/admin` лимитер ставится **до** проверки токена — иначе неудачные попытки не ограничиваются.

При нескольких репликах in-process лимитер даёт лимит × число реплик; поскольку Redis
в проекте уже есть, распределённый вариант (`INCR` + `EXPIRE` на окно) предпочтителен —
и он согласуется с A-05 (Redis как единая точка координации).

### Комментарий

Пункт «нет rate limiting» сам по себе банален, но в этом проекте он мультипликатор: три
других дефекта (E-01 перебор, E-07 брутфорс, E-11 OOM) переходят из «теоретически возможно»
в «практически исполнимо» именно из-за него. Если исправлять раздел частично, лимитер
на `/admin` и на `GET /barcode/:id` даёт наибольший эффект на единицу усилий.

---

## E-13. PII и URL артефактов в stdout-логах

### Суть
Логи содержат `userId`, `buildId`, URL готовых баркодов, а mock-адаптеры печатают
персональные поля. При централизованном сборе логов персональные данные расходятся
по системам, где они не предполагались, и живут по политике retention логов.

### Место
- `internal/adapters/events/mock_publisher.go:46` — `barcode.edited` с `newUrl`
- `internal/adapters/events/mock_publisher.go:54` — `barcode.generated` с `userId`, `url`
- `internal/adapters/events/mock_publisher.go:61` — транзакции: `userId`, `amount`
- `internal/adapters/events/mock_publisher.go:73,83` — уведомления: `userId`, `buildId`
- `internal/adapters/barcodegen/mock_client.go:83,114` — печать `idempotencyKey`
- `internal/adapters/kafka/consumer.go:74` — `batchId`, число items при ошибке

### Подробное описание

Примеры:

```go
log.Printf("event barcode.generated published: userId=%s buildId=%s type=%s url=%s",
	event.UserID, event.BuildID, event.BarcodeType, event.BarcodeURL)
log.Printf("[trans-history] TRANSACTION_LOG → userId=%s type=%s amount=%.2f",
	event.UserID, event.Type, event.Amount)
log.Printf("event barcode.edited published: id=%s field=%s newUrl=%s", ...)
```

Категории риска:

1. **Идентификаторы и URL артефактов.** `barcodeUrl` — прямая ссылка на изображение документа.
   Если хранилище отдаёт по URL без авторизации (типично для CDN), лог становится набором
   рабочих ссылок на документы.
2. **Финансовые данные.** `userId` + `amount` — платёжная активность конкретного пользователя.
3. **`idempotencyKey`.** В сочетании с B-01 (ключ глобальный, без `userID`) знание ключа
   позволяет получить кэшированный чужой ответ. Логирование ключей делает атаку тривиальной.
4. **Прямые PII в mock-режиме.** `history.MockClient` содержит `firstName: JOHN`,
   `dateOfBirth: 1990-05-15`; при `HISTORY_URL` не заданном (E-04-паттерн) реальные значения
   проходят через мок-код.

Часть перечисленного — в mock-адаптерах, то есть в production при полной конфигурации
эти строки не исполняются. Но именно fail-open (E-04/E-06) делает вероятным их выполнение
в проде, поэтому считать их «только dev» нельзя.

Отдельно: логирование не структурировано (`log.Printf`), поэтому выборочная маскировка
полей на стороне сборщика невозможна — только regexp по строкам.

### Трассировка

```
ENV: KAFKA_BROKERS не задан → events.MockPublisher (api_app.go:179)
POST /api/v1/barcode/generate {"units":1,...}
→ PublishBarcodeGenerated(event)
→ stdout: event barcode.generated published: userId=usr_8842 buildId=bld_1193
          type=pdf417 url=https://cdn.example.com/barcodes/bc_77219.png
→ сборщик логов (Loki/CloudWatch/ELK) сохраняет строку с userId и рабочей ссылкой
```

### Предлагаемый багфикс

1. Перейти на структурированное логирование (`log/slog` из stdlib) и явно помечать
   чувствительные поля, чтобы их можно было фильтровать централизованно:

```go
slog.Info("event published",
	slog.String("event", "barcode.generated"),
	slog.String("userId", hashID(event.UserID)),   // хеш вместо сырого ID
	slog.String("buildId", event.BuildID),
	slog.String("barcodeType", event.BarcodeType),
	// URL не логируем: прямая ссылка на артефакт
)
```

2. Ввести helper для усечения идентификаторов и никогда не печатать URL артефактов,
   значения полей документа и idempotency-ключи:

```go
// internal/logging/redact.go

// HashID возвращает короткий стабильный хеш идентификатора для логов.
// Позволяет коррелировать записи, не раскрывая исходный ID.
func HashID(id string) string {
	if id == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:4])
}
```

3. Заменить `log.Printf("...idempotency-key=%s"...)` на логирование только факта
   («ключ присутствует/отсутствует») либо `HashID(key)`.

### Комментарий

Для сервиса, обрабатывающего данные удостоверений личности, объём логируемых PII нужно
приводить к минимуму осознанно, а не по остаточному принципу. Сейчас основной объём —
в mock-адаптерах, где это выглядело безобидно на этапе разработки; fail-open-конфигурация
переносит их в прод. Порядок действий: сначала закрыть E-04/E-06 (моки не попадают в прод),
затем чистить логи.

---

## E-14. `/metrics` и `/health` доступны без аутентификации

### Суть
Оба эндпоинта зарегистрированы до защищённых групп и не требуют токена. `/metrics` раскрывает
операционную статистику, `/health` отвечает `200` безусловно, не проверяя зависимости.

### Место
- `internal/transport/http/gin/router.go:36-39` — регистрация
- `internal/transport/http/gin/middleware.go:26-29` — исключения в maintenance-режиме
- `internal/metrics/metrics.go` — состав метрик

### Подробное описание

```go
r.GET("/health", func(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
})
r.GET("/metrics", gin.WrapH(promhttp.Handler()))
```

`/metrics`: раскрывает `bff_requests_total{endpoint,status}` — карту эндпоинтов и профиль
трафика; `bff_partial_success_total`, `bff_duplicate_requests_total`,
`bff_barcodegen_calls_total{status}` — объёмы бизнес-операций и долю ошибок;
плюс стандартные `go_*`/`process_*` (версия Go, потребление памяти, время старта).
Для внешнего наблюдателя это разведданные: объём бизнеса, окна деплоя, эффект нагрузки.

Что важно отметить как сделанное правильно: метки метрик не содержат `userID` или `barcodeID` —
`endpoint`/`status` дают низкую кардинальность. То есть утечки PII через метрики нет
(в отличие от логов, E-13), и cardinality-бомбы тоже нет.

`/health`: возвращает `{"status":"ok"}` без проверки Redis, Kafka, Billing, BarcodeGen.
Оркестратор считает реплику здоровой, даже если все зависимости недоступны. Это не столько
проблема безопасности, сколько ложный сигнал в мониторинге; вместе с A-06 (shutdown)
и E-06 (mock в проде) означает, что «зелёный» `/health` не гарантирует работоспособности.

### Трассировка

```
GET /metrics                        (без Authorization)
router.go:39   promhttp.Handler()   → 200, полный дамп метрик
GET /health                         (без Authorization)
router.go:36   → 200 {"status":"ok"}   ← Redis/Kafka/Billing не проверялись

MAINTENANCE_MODE=true:
middleware.go:26  FullPath()=="/health" || "/metrics" → c.Next()  (исключены намеренно)
```

### Предлагаемый багфикс

1. Не публиковать `/metrics` наружу. Предпочтительно — отдельный слушатель на internal-порту:

```go
// cmd/api/main.go — метрики на отдельном порту, не публикуемом ingress-ом
go func() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{
		Addr:              ":9090",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("metrics server: %v", err)
	}
}()
```

Если разделить порты нельзя — закрыть тем же service-токеном:
`r.GET("/metrics", ServiceJWTMiddleware(internalJWT), gin.WrapH(promhttp.Handler()))`.

2. Разделить liveness и readiness:

```go
// /health — liveness: процесс жив, зависимости не проверяются (для рестартов).
r.GET("/health", func(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
})

// /ready — readiness: реальная проверка зависимостей (для балансировки трафика).
r.GET("/ready", func(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	checks := map[string]string{}
	httpStatus := http.StatusOK
	for name, probe := range readinessProbes {   // redis, kafka, billing, barcodegen
		if err := probe(ctx); err != nil {
			checks[name] = "fail: " + err.Error()
			httpStatus = http.StatusServiceUnavailable
			continue
		}
		checks[name] = "ok"
	}
	c.JSON(httpStatus, gin.H{"checks": checks})
})
```

`/ready` не должен раскрывать детали ошибок наружу — если он публичен, возвращать только
имена компонентов и статус без текста ошибки.

### Комментарий

Исключение `/health` и `/metrics` из maintenance-режима сделано правильно — иначе оркестратор
убил бы реплики во время обслуживания. Проблема только в публичной доступности `/metrics`
и в том, что `/health` не отвечает на вопрос, который ему задают. Отсутствие PII в метках
метрик — сознательно хорошая работа, это стоит сохранить при добавлении новых метрик
(в частности, не добавлять `userID` в `bff_adapter_mock_mode` и подобные).

---

## E-15. Нет CORS-политики и security-заголовков

### Суть
Роутер не задаёт ни CORS, ни `X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`,
`Strict-Transport-Security`. Для BFF, вызываемого браузерным фронтендом, политика CORS должна
быть явной, а не «по умолчанию».

### Место
- `internal/transport/http/gin/router.go:31-34` — цепочка middleware
- `internal/transport/http/gin/server.go` — конструирование `http.Server`

### Подробное описание

Отсутствие CORS-middleware означает, что заголовки `Access-Control-Allow-*` не отправляются.
Практические следствия:

- Браузер блокирует cross-origin запросы к BFF. Если фронтенд на другом домене — он не работает,
  и обычная «быстрая» реакция на это (`Access-Control-Allow-Origin: *`) создаёт настоящую
  уязвимость. Явная политика с whitelist предотвращает такой сценарий.
- Preflight `OPTIONS` не обрабатывается: Gin ответит `404`.

Отсутствие security-заголовков менее критично для чистого JSON API, но `X-Content-Type-Options: nosniff`
и `Referrer-Policy` дешевы и уместны; `Strict-Transport-Security` нужен, если BFF доступен
напрямую по HTTPS (а не только через ingress, который его добавляет).

Смежно: `RespondError` возвращает `AppError.Message`, который может содержать текст ошибки
downstream (см. E-16) — при наличии CORS `*` это становится читаемым для любого сайта.

### Трассировка

```
Браузер (https://app.example.com) → POST https://bff.example.com/api/v1/barcode/generate
1) Preflight: OPTIONS /api/v1/barcode/generate
   Gin: маршрут OPTIONS не зарегистрирован → 404
2) Браузер блокирует основной запрос: нет Access-Control-Allow-Origin
Ответы BFF также не содержат X-Content-Type-Options / Referrer-Policy / HSTS.
```

### Предлагаемый багфикс

Явный whitelist источников из ENV, без wildcard:

```go
// internal/transport/http/gin/middleware.go

// CORSMiddleware отдаёт CORS-заголовки только для источников из whitelist.
// Wildcard не поддерживается намеренно: BFF работает с аутентифицированными запросами.
func CORSMiddleware(allowedOrigins []string) gin.HandlerFunc {
	allowed := make(map[string]bool, len(allowedOrigins))
	for _, o := range allowedOrigins {
		allowed[strings.TrimSpace(o)] = true
	}
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if origin != "" && allowed[origin] {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Vary", "Origin")
			c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
			c.Header("Access-Control-Allow-Headers",
				"Authorization, Content-Type, X-Idempotency-Key")
			c.Header("Access-Control-Max-Age", "600")
		}
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

// SecurityHeadersMiddleware добавляет базовые защитные заголовки ответа.
func SecurityHeadersMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("Referrer-Policy", "strict-origin-when-cross-origin")
		c.Header("X-Frame-Options", "DENY")
		c.Next()
	}
}
```

Подключение и новая ENV `CORS_ALLOWED_ORIGINS` (список через запятую; пустой = CORS выключен):

```go
r.Use(SecurityHeadersMiddleware())
if len(corsOrigins) > 0 {
	r.Use(CORSMiddleware(corsOrigins))
}
```

`Access-Control-Allow-Credentials` не включать, пока аутентификация идёт через
`Authorization`-заголовок, а не cookie.

### Комментарий

Пункт «средний» именно потому, что сейчас отсутствие CORS работает как запрет — небезопасного
поведения нет. Риск в реакции на первый же отчёт «фронтенд не может позвать BFF»: типовое
исправление `AllowAllOrigins` вместе с E-16 (детали ошибок в теле) и E-01 (чтение любого
баркода) даёт полноценную кражу данных через браузер жертвы. Лучше внести явную политику
заранее.

---

## E-16. Детали внутренних ошибок утекают клиенту

### Суть
`domain.NewBarcodeGenError(err)` и аналогичные конструкторы переносят текст ошибки downstream
в поле ответа. Клиент видит внутренние адреса, коды и сообщения чужих сервисов.

### Место
- `internal/transport/http/gin/api_handler.go:145` — `RespondError(c, domain.NewBarcodeGenError(err))` в `GetBarcode`
- `internal/transport/http/gin/api_handler.go:174,193` — то же в `GeneratePDF417`/`GenerateCode128`
- `internal/usecase/edit_usecase.go:167` — `"failed to publish barcode.edited event: " + err.Error()`
- `internal/domain/apperrors.go` — конструкторы `AppError`
- `internal/adapters/*/http_client.go` — формирование текста ошибок с URL и статусами

### Подробное описание

Ошибки транспортного уровня оборачиваются, но не обезличиваются. HTTP-клиенты формируют
сообщения, включающие адрес и статус вызванного сервиса; usecase добавляет
`err.Error()` в `AppError.Message`; `RespondError` отдаёт `Message` клиенту.

Что может попасть в ответ:

- внутренние hostname и порты (`http://barcodegen:8080`, `http://billing:3000`) —
  карта внутренней сети;
- статусы и тела ответов downstream — версии, стектрейсы, имена таблиц;
- в `edit_usecase.go:167` — ошибки Kafka: адреса брокеров, имена топиков.

Дополнительно `GetBarcode` оборачивает ошибку History в `NewBarcodeGenError` — то есть
классификация неверна (это не ошибка BarcodeGen), что искажает и код ответа, и диагностику.
При исправлении E-01 эта ветка всё равно требует переработки: «не найдено» и «нет доступа»
должны давать `404`, а не `BARCODEGEN_ERROR`.

### Трассировка

```
GET /api/v1/barcode/bc_1  (History недоступен)
history/http_client.go   err = "history: get barcode: Post \"http://history:3000/api/v1/...\":
                                dial tcp 10.0.3.17:3000: connect: connection refused"
api_handler.go:145       RespondError(c, domain.NewBarcodeGenError(err))
→ HTTP 503 {"code":"BARCODEGEN_ERROR",
            "message":"history: get barcode: Post \"http://history:3000/...\":
                       dial tcp 10.0.3.17:3000: connect: connection refused"}
   ↑ внутренний hostname, внутренний IP, порт — в ответе публичного API
```

### Предлагаемый багфикс

Разделить внутреннюю диагностику и клиентское сообщение: детали — в лог с `traceID`,
клиенту — обезличенный текст и идентификатор для поддержки.

```go
// internal/domain/apperrors.go

// AppError.Internal — детали для логов, никогда не сериализуются в ответ.
type AppError struct {
	Code       string         `json:"code"`
	HTTPStatus int            `json:"-"`
	Message    string         `json:"message"`
	Details    map[string]any `json:"details,omitempty"`
	Internal   error          `json:"-"`   // ← не уходит клиенту
}

func NewBarcodeGenError(err error) *AppError {
	return &AppError{
		Code:       ErrCodeBarcodeGenError,
		HTTPStatus: 503,
		Message:    "barcode generation service is temporarily unavailable",
		Internal:   err,
	}
}
```

```go
// internal/transport/http/gin/error_handler.go
func RespondError(c *gin.Context, err error) {
	var appErr *domain.AppError
	if errors.As(err, &appErr) {
		traceID := c.GetString("traceID")
		if appErr.Internal != nil {
			log.Printf("error: trace=%s code=%s path=%s internal=%v",
				traceID, appErr.Code, c.FullPath(), appErr.Internal)
		}
		c.JSON(appErr.HTTPStatus, gin.H{
			"code":    appErr.Code,
			"message": appErr.Message,   // только безопасный текст
			"traceId": traceID,
			"details": appErr.Details,
		})
		return
	}
	log.Printf("error: unhandled path=%s err=%v", c.FullPath(), err)
	c.JSON(http.StatusInternalServerError, gin.H{
		"code": "INTERNAL_ERROR", "message": "internal server error",
	})
}
```

Исключение — `VALIDATION_ERROR`: там текст описывает ошибку клиента и должен оставаться
информативным. Правило: сообщения о **своих** ошибках клиента раскрываем, о **чужих**
внутренних сбоях — нет.

### Комментарий

Обратная сторона аккуратной обработки ошибок: `AppError` с `Code`/`HTTPStatus`/`Message`
спроектирован хорошо, но у него нет разделения на «для клиента» и «для лога», и поэтому
`err.Error()` естественным образом попадает в публичное поле. Добавление поля `Internal`
решает вопрос централизованно, без правки каждого вызова. Смежно с E-15: при разрешающем
CORS эти тексты становятся доступны сторонним сайтам.

---

## E-17. `BILLING_INTERNAL_API_KEY` с дефолтом `dev-billing-key`

### Суть
Ключ для заголовка `x-internal-api-key` при вызовах BFF → Billing имеет dev-дефолт.
Если переменная не задана, BFF обращается к биллингу с известным из репозитория ключом.

### Место
- `internal/config/config.go:141` — `BillingInternalKey: getEnv(EnvBillingInternalKey, "dev-billing-key")`
- `internal/adapters/billing/http_client.go` — установка заголовка `x-internal-api-key`
- `internal/app/api_app.go:121-124` — передача ключа в клиент

### Подробное описание

Тот же паттерн, что E-03, но для service-to-service вызова. Комментарий в конфиге фиксирует,
что значение должно совпадать с `INTERNAL_API_KEY` на стороне Billing (со ссылкой на
`fix_for_services.md`). Значит, ключ — общий секрет двух сервисов, и его дефолт публично известен.

Возможные последствия:

- если Billing принимает `dev-billing-key` (например, тоже поднят с дефолтом), любой, кто
  может обратиться к Billing по сети, выполняет операции от имени BFF: `Block`, `Capture`,
  `Release` — то есть управляет средствами;
- если Billing ключ не принимает, все вызовы падают с ошибкой авторизации; в сочетании
  с E-06 сценарий «BILLING_URL не задан → mock» выглядит для оператора как рабочий сервис,
  а не как ошибка конфигурации.

Наблюдаемость нулевая: несовпадение ключей проявится как ошибки биллинга, а работа на
дефолтном ключе — вообще никак.

### Трассировка

```
ENV: BILLING_URL=http://billing:3000
     BILLING_INTERNAL_API_KEY не задана
config.go:141  BillingInternalKey = "dev-billing-key"
api_app.go:122 billing.NewHTTPClient(url, timeout, "dev-billing-key")
               log: "billing: using HTTP client → http://billing:3000"   ← выглядит штатно

POST /api/v1/barcode/generate
→ BFF → Billing: POST /api/v1/block
        x-internal-api-key: dev-billing-key
  Вариант A: Billing принимает dev-ключ → секрет-заглушка защищает деньги
  Вариант B: Billing отклоняет → 401/403 от биллинга при штатном с виду старте
```

### Предлагаемый багфикс

Включить ключ в strict-валидацию (см. E-03) и не давать ему dev-дефолта в production:

```go
// в Config.Validate()
if c.Services.BillingInternalKey == "" || c.Services.BillingInternalKey == "dev-billing-key" {
	problems = append(problems, "BILLING_INTERNAL_API_KEY must be set to a non-default value")
}
```

Дополнительно — проверять согласованность при старте (быстрый fail вместо ошибок под нагрузкой):
сделать один health-вызов к Billing и залогировать/прервать старт при `401`/`403`.
Стратегически — mTLS между внутренними сервисами вместо общего статического ключа.

### Комментарий

Четвёртый экземпляр одного и того же дефекта (после `JWT_SECRET`, `ADMIN_JWT`,
`INTERNAL_SERVICE_JWT`) — это уже не отдельные недочёты, а системное свойство конфигурации:
все секреты имеют работающие dev-дефолты, и ни один не проверяется при старте. Одна функция
`Config.Validate()` закрывает E-03, E-04, E-05, E-06 и E-17 одновременно — это самая
выгодная правка раздела.

---

## E-18. Path traversal в `PUT /admin/revisions/:revision` — не воспроизводится ⚪

### Суть
Проверялась гипотеза: `:revision` из URL попадает в имя файла при сохранении YAML, поэтому
`../../` может привести к записи за пределы `configs/revisions`. **Уязвимость отсутствует** —
имя файла берётся не из параметра запроса.

### Место
- `internal/transport/http/gin/admin_handler.go:87-101` — `UpdateRevision`
- `internal/adapters/revisions/memory_store.go:117-131` — `UpdateConfig`
- `internal/adapters/revisions/memory_store.go:155-196` — `persistConfigLocked`

### Подробное описание

Цепочка выглядит опасной: `revision := c.Param("revision")` → `UpdateConfig(ctx, revision, req)`
→ запись файла. Однако в `UpdateConfig` параметр используется только как ключ поиска
в существующей мапе:

```go
func (s *MemoryStore) UpdateConfig(_ context.Context, name string, req domain.UpdateRevisionRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg, ok := s.configs[name]
	if !ok {
		return fmt.Errorf("revision %q not found", name)   // ← неизвестное имя отсекается
	}
	updated := cfg
	...
	if err := s.persistConfigLocked(updated); err != nil {
```

А путь формируется из `cfg.Name`, то есть из значения, взятого **из мапы**, а не из запроса:

```go
path := filepath.Join(s.dir, cfg.Name+".yaml")
if err := os.WriteFile(path, payload, 0o644); err != nil {
```

Два барьера: (1) `name` должен существовать как ключ, иначе ранний выход с ошибкой;
(2) в путь идёт `cfg.Name`, а не входной параметр. Даже если бы ключ совпал, имя файла
взялось бы из доверенного источника.

### Трассировка

```
PUT /admin/revisions/..%2f..%2f..%2fetc%2fcron.d%2fpwn
Authorization: Bearer dev-admin-token
{"enabled":true,"calculationChain":[]}

admin_handler.go:88   revision = "../../../etc/cron.d/pwn"   (Gin декодирует %2f)
admin_handler.go:99   h.revisions.UpdateConfig(ctx, "../../../etc/cron.d/pwn", req)
memory_store.go:120   s.configs["../../../etc/cron.d/pwn"] → ok == false
memory_store.go:122   return fmt.Errorf("revision %q not found", name)
→ HTTP 4xx, запись файла не выполнялась
```

### Предлагаемый багфикс

Изменение кода не требуется. Для устойчивости к будущему рефакторингу (если появится
создание новых ревизий, где имя придёт от клиента) стоит зафиксировать инвариант тестом
и добавить дешёвую проверку в момент записи:

```go
func (s *MemoryStore) persistConfigLocked(cfg domain.RevisionConfig) error {
	if s.dir == "" {
		return nil
	}
	// Инвариант: имя ревизии — простой идентификатор, а не путь.
	if cfg.Name == "" || strings.ContainsAny(cfg.Name, `/\`) || strings.Contains(cfg.Name, "..") {
		return fmt.Errorf("invalid revision name for persistence: %q", cfg.Name)
	}
	...
}
```

Плюс тест: `UpdateConfig(ctx, "../../etc/passwd", req)` → ошибка, файлов за пределами
`s.dir` не появилось.

### Комментарий

Защита здесь возникла как побочный эффект архитектурного решения «ревизии предзаданы,
создание новых не поддерживается», а не как осознанная валидация пути. Это работает, но
хрупко: как только появится `POST /admin/revisions` для новой ревизии, барьер «ключ должен
существовать» исчезнет, и traversal станет реальным. Поэтому — проверка на уровне записи
и тест, фиксирующий инвариант.

---

## E-19. SSRF через `signatureUrl`/`photoUrl` — в BFF отсутствует ⚪

### Суть
Проверялась гипотеза: URL, приходящие от AI Service (или от клиента в `fields`), запрашиваются
BFF, что даёт SSRF. **В BFF таких запросов нет** — URL только передаются дальше как данные.

### Место
- `internal/adapters/ai/http_client.go` — `GenerateSignature`, `GeneratePhoto`
- `internal/usecase/generate_usecase.go` — размещение URL в `fields`
- `internal/adapters/barcodegen/http_client.go` — передача `fields` в BarcodeGen

### Подробное описание

Поиск по кодовой базе показывает: единственные исходящие HTTP-вызовы — к фиксированным
адресам из конфигурации (`BARCODEGEN_URL`, `BILLING_URL`, `AI_URL`, `HISTORY_URL`).
Ни один клиент не строит запрос по URL, полученному из тела запроса или из ответа
downstream-сервиса. `signatureUrl`/`photoUrl` попадают в `fields` и уходят в BarcodeGen
как строки — BFF их не загружает, не парсит и не валидирует.

Следствие: SSRF-поверхности в BFF нет. Однако риск переносится на потребителя:

- BarcodeGen, встраивающий изображение по URL, обращается к внешнему адресу — SSRF-поверхность
  находится там;
- URL от AI Service принимаются на доверии; если AI скомпрометирован или ошибается,
  BFF передаст произвольный URL дальше без проверки схемы и хоста.

Это вопрос модели доверия между сервисами, а не дефект BFF. Но поскольку BFF — единственная
точка, через которую URL проходят, дешёвая валидация здесь защищает всю цепочку.

### Трассировка

```
POST /api/v1/barcode/generate {"generateSignature":true,...}
generate_usecase → ai.GenerateSignature(...)          → AI_URL (фиксированный адрес)
AI отвечает: {"signatureUrl":"http://169.254.169.254/latest/meta-data/"}
generate_usecase   fields["signatureUrl"] = "http://169.254.169.254/..."   ← только запись в map
barcodegen.Generate(fields)                            → BARCODEGEN_URL (фиксированный адрес)
BFF по этому URL не обращается. Загрузит ли его BarcodeGen — вне зоны BFF.
```

### Предлагаемый багфикс

Изменение обязательным не является. Рекомендуется allow-list как защита downstream:

```go
// internal/domain/validate.go

var allowedAssetHosts = map[string]bool{
	"cdn.example.com":    true,
	"assets.example.com": true,
}

// ValidateAssetURL проверяет, что URL артефакта указывает на разрешённый хост по HTTPS.
// Защищает downstream-сервисы (BarcodeGen) от обращения к внутренним адресам.
func ValidateAssetURL(raw string) error {
	if raw == "" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return NewValidationError("invalid asset URL")
	}
	if u.Scheme != "https" {
		return NewValidationError("asset URL must use https")
	}
	if !allowedAssetHosts[u.Hostname()] {
		return NewValidationError("asset URL host is not allowed")
	}
	return nil
}
```

Применять к `signatureUrl`/`photoUrl` и после ответа AI, и при приёме от клиента.
Хосты — в ENV, чтобы список не был захардкожен.

### Комментарий

Полезный негативный результат: он очерчивает границу ответственности. BFF действительно
остаётся оркестратором и не выступает HTTP-клиентом произвольных адресов — это архитектурно
верно. Валидацию стоит добавить именно потому, что BFF — единственное место, где URL из
разных источников сходятся вместе, и одна проверка закрывает риск для всех потребителей.

---

## E-20. JWT alg confusion — отклоняется корректно ⚪

### Суть
Проверялись классические обходы подписи: `alg=none` и подмена HMAC на асимметричный алгоритм.
Оба **отклоняются**.

### Место
- `internal/adapters/auth/local_validator.go:29-34` — keyfunc с проверкой метода

### Подробное описание

```go
parsed, err := jwt.Parse(token, func(t *jwt.Token) (interface{}, error) {
	if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
		return nil, fmt.Errorf("auth: unexpected signing method: %v", t.Header["alg"])
	}
	return v.secret, nil
})
```

Проверка типа метода до возврата ключа закрывает оба вектора: `alg=none` не является
`*jwt.SigningMethodHMAC`; подставить `alg=RS256`, чтобы секрет интерпретировался как публичный
ключ, тоже нельзя. Дополнительно `golang-jwt/v5` сам по себе запрещает `none` без явного
`UnsafeAllowNoneSignatureType`.

### Трассировка

```
PROBE E-05: alg=none → err=auth: invalid token: token is unverifiable:
            error while executing keyfunc: auth: unexpected signing method: none
PROBE E-02b: expired token → err=auth: invalid token: token has invalid claims: token is expired
```

Проверка алгоритма и проверка `exp` (когда claim присутствует) работают.

### Предлагаемый багфикс

Не требуется. При исправлении E-09 ручную проверку метода стоит заменить на декларативную,
не ослабляя её:

```go
opts := []jwt.ParserOption{
	jwt.WithValidMethods([]string{"HS256", "HS384", "HS512"}),   // эквивалент проверки в keyfunc
	jwt.WithExpirationRequired(),
}
```

Важно: если keyfunc упростят до `return v.secret, nil` **без** добавления `WithValidMethods`,
защита от alg confusion исчезнет. Это стоит закрыть регрессионным тестом на `alg=none`.

### Комментарий

Место сделано правильно, и это заметно на фоне остальных пунктов: автор знал про alg confusion.
Отмечаю в отчёте, чтобы при рефакторинге по E-09 свойство не потерялось — типичная регрессия
как раз здесь.

---

## E.99. Порядок исправления

Зависимости между пунктами учтены: сначала то, что делает остальное осмысленным.

| Шаг | Что делаем | Закрывает | Обоснование |
|-----|-----------|-----------|-------------|
| 1 | `Config.Validate()` + `APP_ENV=production`, отказ старта на dev-дефолтах и отсутствующих URL | E-03, E-04, E-05, E-06, E-17 | Одна функция закрывает 5 дефектов, из них 4 критичных. Без этого остальные правки обходятся конфигурацией |
| 2 | Проверка владельца в `GetBarcode` и `EditUseCase.Execute` | E-01, E-02 | Два критичных IDOR; делать одним изменением, иначе закроют только read-путь |
| 3 | `panic` на пустой токен + отказ на пустом Bearer в middleware | E-08 | Две строки, снимает зависимость безопасности от `getEnv` |
| 4 | `jwt.WithExpirationRequired()`, `WithValidMethods`, `iss`/`aud` | E-09, сохранить E-20 | Делает возможным отзыв токенов; регрессионный тест на `alg=none` |
| 5 | Лимит размера тела + rate limiting (начать с `/admin` и `GET /barcode/:id`) | E-11, E-12 | Убирает мультипликатор для E-01 и E-07 |
| 6 | `/metrics` на отдельный порт, `/health` → `/health` + `/ready` | E-14 | Также исправляет ложный «зелёный» статус из A-06 |
| 7 | `AppError.Internal` + аудит-логи в admin-middleware | E-16, частично E-07 | Прекращает утечку внутренней топологии, даёт аудит admin-действий |
| 8 | Разделение service-секретов (`BULK_SERVICE_JWT`) или scopes | E-10 | Ограничивает радиус компрометации одного клиента |
| 9 | Структурированные логи с редактированием PII | E-13 | После шага 1 (моки не в проде) объём проблемы меньше |
| 10 | Явный CORS whitelist + security-заголовки | E-15 | Делается до того, как «фронтенд не работает» приведёт к `AllowAllOrigins` |
| 11 | Тесты-инварианты на path traversal и валидация asset-URL | E-18, E-19 | Фиксация текущего безопасного состояния против регрессий |

### Напоминание по состоянию репозитория

HEAD (`e3b3be7`) **не собирается**: `internal/app/api_app.go:219` вызывает
`kafkaadapter.NewConsumer` с 3 аргументами вместо 4 (после добавления DLQ). Пакеты
`cmd/api`, `cmd/worker`, `internal/app` не компилируются, поэтому `go build ./...`,
`go vet ./...` и тесты этих пакетов красные. Правка — один аргумент; сделать до начала
работ по разделу, иначе изменения в `api_app.go` (шаг 1) нельзя проверить сборкой.
