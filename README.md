# xk6-wsshared — универсальный WebSocket-клиент для k6 с обработкой в Go

`import ws from 'k6/x/wsshared'`

Расширение для нагрузочных тестов WebSocket-сервисов с плотным потоком сообщений.
Протоколом приложения не ограничено: формат кадров, автоответы на keep-alive,
извлекаемые поля и фильтры описываются в скрипте.

## Зачем

В штатном `k6/ws` каждое сообщение будит JS. Goja — интерпретатор без JIT,
поэтому распаковка, разрезание и разбор большого payload на каждом сообщении
каждого VU съедают генератор раньше, чем нагрузка дойдёт до сервера.

Здесь на Go вынесено всё, что происходит на каждом сообщении:

| Что | Как в скрипте |
|---|---|
| кадрирование: сжатие, VarInt-длина, разделитель | `codec` |
| keep-alive: ответ на ping, периодический heartbeat | `autoReply`, `heartbeat` |
| извлечение полей, задержка по таймштампу | `fields` |
| jsonPath и регулярки | `fields`, матчеры, `ev.jsonPath()`, `ev.match()` |
| фильтрация: «нужны только такие сообщения» | `collect()`, `waitFor()`, `when` |
| последнее значение «на сейчас» | `track` → `tracked()` |
| последние N сообщений, забрать когда удобно | `buffers` → `buffer()` |
| перезапуск соединения с повтором подписок | `reconnect`, `onOpen`, `send(..., {replay})` |

JS просыпается, только когда скрипт сам просит данные.

## Что в папке

| Путь | Что это |
|---|---|
| `*.go`, [`binfmt/`](binfmt) | само расширение и форматы MessagePack/protobuf |
| [`examples/`](examples) | моки `mockws`, `mockhub` и сценарии k6 к ним |
| [`xk6-shared/`](xk6-shared) | копия расширения `k6/x/shared` (общее состояние между VU), отдельный Go-модуль; нужна для сборки k6 и для `examples/hubs.js` |
| [`.claude/skills/xk6-qa/`](.claude/skills/xk6-qa/SKILL.md) | скилл Claude Code для проверки xk6-расширений: статика, JS-уровень, `-race`, goleak, фаззинг, e2e. Подхватывается сам, если открыть папку в Claude Code; вызов `/xk6-qa` |

Нужен Go 1.25.6+. Для `go test -race` на Windows — gcc (портативный MinGW-w64,
см. скилл, §3).

---

## Сборка

Из корня этой папки (bash / Git Bash):

```bash
xk6 build v1.1.0 \
  --with github.com/altenar/xk6-wsshared="$(pwd -W)" \
  --with github.com/altenar/xk6-shared="$(pwd -W)/xk6-shared" \
  --output ./k6-wsshared.exe
```

> Windows: путь только нативный (`pwd -W` → `C:/...`). Если `xk6 build` падает
> с невнятным `exit status 1`, соберите вручную, тогда ошибка будет видна:
> ```bash
> mkdir build && cd build
> printf 'package main\nimport (\n _ "github.com/altenar/xk6-shared"\n _ "github.com/altenar/xk6-wsshared"\n "go.k6.io/k6/cmd"\n)\nfunc main() { cmd.Execute() }\n' > main.go
> go mod init build
> go mod edit -require=go.k6.io/k6@v1.1.0 \
>   -require=github.com/altenar/xk6-wsshared@v0.0.0 \
>   -require=github.com/altenar/xk6-shared@v0.0.0 \
>   -replace=github.com/altenar/xk6-wsshared="$(cd .. && pwd -W)" \
>   -replace=github.com/altenar/xk6-shared="$(cd .. && pwd -W)/xk6-shared"
> go mod tidy && go build -o ../k6-wsshared.exe .
> ```
> `go mod edit` запускайте из bash: PowerShell портит аргументы вида `-require=x@v`.

Проверка: `./k6-wsshared.exe version` должен показать `k6/x/wsshared [js]`.
В скрипте доступны `ws.version` и `ws.features`.

---

## Быстрый старт

```js
import ws from 'k6/x/wsshared';

export default function () {
    const conn = ws.connect({
        url: 'wss://host/ws',
        headers: { Authorization: `Bearer ${token}` },
        typePath: 'type',                                    // ev.type + тег метрик
        onOpen: [{ op: 'auth', token }],                     // на каждый (ре)коннект
        autoReply: [{ match: { type: 'ping' }, reply: { type: 'pong' } }],
        fields: [{ name: 'ts', fromEnd: true, asTimestamp: true }],
    });

    conn.send({ op: 'subscribe', channel: 'quotes.EURUSD' }, { replay: true });
    conn.run(45);          // 45 с: всё в Go, JS не просыпается
    conn.close();
}
```

---

## `ws.connect(config)` → `Conn`

### Подключение

| Поле | Тип | По умолчанию | Смысл |
|---|---|---|---|
| `url` | string | **обязательно** | `ws://`/`wss://` (`http(s)://` сконвертируется) |
| `headers` | object | — | заголовки апгрейда |
| `query` | object | — | query-параметры (значения percent-кодируются) |
| `subprotocols` | string[] | — | `Sec-WebSocket-Protocol`; выбранный — в `conn.subprotocol` |
| `enableCompression` | bool | `false` | WS-расширение permessage-deflate |
| `skipTLSVerify` | bool | `false` | не проверять сертификат |
| `handshakeTimeoutMs` | number | 45000 | таймаут апгрейда |

### `codec` — формат кадров

Три независимых слоя, по умолчанию все выключены: один текстовый кадр — одно
сообщение.

```js
codec: {
    compression: 'deflate',   // 'none' | 'deflate' (raw) | 'zlib' | 'gzip'
    lengthPrefix: 'varint',   // 'none' | 'varint'
    delimiter: '\x1e',        // '' | '\n' | любой разделитель сообщений
    binary: true,             // тип исходящего кадра; по умолчанию binary,
                              // если включено сжатие или префикс
    strict: false,            // см. ниже
}
```

Исходящее: `payload + delimiter → compress → [varint-длина]`.
Входящее — в обратном порядке. Один кадр может нести несколько сообщений.

**`strict: false` (по умолчанию).** Если кадр не распаковывается или не парсится
как length-prefixed, он берётся как есть, и `stats().fallbacks` растёт. Так
бывает в жизни: сервер шлёт plain-text ack по каналу, который в остальном сжат.
С `strict: true` такой кадр превращается в событие с `error`.

### `codec.format` — бинарные форматы (MessagePack, protobuf)

Самый внутренний слой кодека. Входящая запись один раз переводится в JSON
прямо в read-loop, и дальше всё работает как с обычным JSON: `typePath`,
`fields`, `track`, `buffers`, матчеры, `ev.jsonPath()`, регулярки, задержки.
Исходящее сообщение пишется в JSON и уходит в формате канала.

```
исходящее:  JSON → format → [сжатие] → [varint-длина]
входящее:   [varint-длина] → [распаковка] → format → JSON
```

```js
// MessagePack — без схемы
codec: { format: 'msgpack', lengthPrefix: 'varint' }

// protobuf — по схеме: .proto текстом, protoc не нужен
const PROTO = open('./api.proto');                  // init-контекст
codec: {
    format: 'protobuf',
    proto: {
        source: PROTO,                // или descriptorSet: open('./api.pb', 'b')
        message: 'shop.Envelope',     // входящий тип, полное имя
        sendMessage: 'shop.Request',  // исходящий, если отличается
        imports: { 'common/money.proto': open('./common/money.proto') },
        // jsonNames: true            — поля в lowerCamelCase вместо имён из .proto
        // emitDefaults: true         — показывать поля с нулевыми значениями
    },
}
```

**Как бинарь выглядит в JSON**

| MessagePack | JSON |
|---|---|
| int, uint, float, bool, nil | число, `true`/`false`, `null` |
| NaN, ±Inf | `"NaN"`, `"Infinity"`, `"-Infinity"` |
| str | строка, битый UTF-8 заменяется на U+FFFD |
| bin | строка base64; на отправку — `{ $bin: '<base64>' }` |
| ext −1 (timestamp) | строка RFC 3339 в UTC, годится для `asTimestamp` |
| прочие ext | `{"$ext": тип, "$data": "<base64>"}`, на отправку так же |
| ключ map не строка | его JSON-текст строкой: `1` → `"1"` |

Корень может быть массивом: `typePath: '[0]'`, `jsonPath('[2].items[-1]')`.

Protobuf следует стандартному JSON-маппингу: `int64`/`uint64` — **строки**
(`"seq":"42"`), enum — имена, `bytes` — base64. Поля с нулевым значением в
proto3 не выводятся, кроме объявленных `optional`, или включите
`emitDefaults`. Матчеры сравнивают нестрого, так что `{ field: 'seq',
equals: 42 }` работает и со строкой `"42"`. Схема компилируется один раз и
кешируется по содержимому, поэтому `connect` на каждой итерации не платит за
компиляцию.

**Правила слоя**

- Формат применяется только к **бинарным** кадрам. Текстовый кадр на том же
  канале (ack, ошибка) берётся как есть: `{` — валидное число в MessagePack,
  и его «успешное» декодирование дало бы мусор.
- Не разобралось: при `strict: false` запись остаётся как есть, растёт
  `stats().fallbacks`; при `strict: true` получается событие с `error`.
- **`delimiter` с форматом запрещён.** Бинарная запись может содержать байт
  разделителя. Границы задаются `lengthPrefix: 'varint'` или «одно сообщение
  на кадр».
- `sequence: true` (msgpack): одна запись — несколько склеенных значений,
  каждое становится отдельным сообщением.
- Исходящие `onOpen`, `autoReply.reply` и `heartbeat.send` переводятся в формат
  при `connect`, поэтому ошибка схемы видна сразу, а не теряется в read-loop.
  `{ raw: true }` отправляет байты мимо всех слоёв.
- `ev.size` — размер JSON-вида, `ev.wireBytes` — кадр на проводе.
- `ws.codec({ format, … })` работает и без сокета: `decodeBase64(...)` на
  кадре, скопированном из DevTools, сразу показывает JSON.

**Цена** (Go 1.25, i7, заказ из трёх позиций): MessagePack → JSON — 0.46 мкс
и 2 аллокации; protobuf → JSON — 9.6 мкс и 107 аллокаций. Protobuf ограничен
скоростью `protojson`: это примерно 100 тысяч сообщений в секунду на ядро.

### `typePath` — тип сообщения

jsonPath к полю, которое различает виды сообщений, например `'type'`, `'op'`
или `'event'`. Значение попадает в `ev.type`, в матчер `{ type: ... }` и в тег
`messageType` метрики `wsshared_messages_by_type`.

> Путь должен указывать на **вид** сообщения, а не на его id. Различных
> значений тега не больше 64, остальные пишутся как `other`.

### Поведение очереди

Между read-loop и VU есть очередь событий.

| Поле | По умолчанию | Смысл |
|---|---|---|
| `eventBuffer` | 256 | размер очереди |
| `overflow` | `'block'` | что делать, когда очередь полна |
| `queue` | `true` | `false` — очереди нет совсем |
| `keepBody` | `false` | событиям доступно тело (`body()`, `jsonPath()`, …) |

| `overflow` | Поведение | Когда нужен |
|---|---|---|
| `block` | read-loop ждёт VU, давление уходит на сокет | VU читает всё подряд (`run`/`next`/`collect`) |
| `dropOldest` | выкидывается старейшее событие | VU читает изредка, важны свежие |
| `dropNewest` | выкидывается новое событие | важны первые |

`queue: false` — для VU, который пользуется только `track`/`buffers`. Сообщения
разбираются, считаются и раскладываются, но в очередь не попадают. Читать сокет
продолжает Go, обратного давления нет.

> **Ловушка `block`.** Если VU долго занят HTTP и не читает сокет, очередь
> заполняется, read-loop встаёт, а с ним замирают `track`, `buffers` и
> автоответы на ping. Сервер может закрыть соединение по keep-alive. Для
> такого VU берите `dropOldest` или `queue: false`.

### `fields` — извлечение в Go

Значения приходят в каждом событии строками: `ev.fields.name`.

```js
fields: [
    { name: 'ts', fromEnd: true, asTimestamp: true },  // поиск ключа "ts" на любой глубине
    { name: 'sym', key: 'symbol' },                    // имя в событии ≠ ключ в JSON
    { name: 'bid', path: 'data.bid' },                 // jsonPath
    { name: 'seq', regex: '"seq":(\\d+)' },            // регулярка, группа 1
    { name: 'raw', regex: 'id=\\w+', group: 0 },       // всё совпадение
]
```

| Опция | Смысл |
|---|---|
| `name` | имя в `ev.fields` |
| `key` | JSON-ключ для побайтового поиска (по умолчанию = `name`) |
| `path` | jsonPath вместо поиска ключа |
| `regex` / `group` | регулярка по телу; по умолчанию группа 1, если она есть, иначе всё совпадение |
| `fromEnd` | искать ключ с конца — для полей в хвосте большого тела |
| `asTimestamp` | посчитать задержку `приём − значение` → `ev.delays.name` и `wsshared_message_delay_ms{field}` |

**Таймштампы:** ISO-8601 с зоной или без (без зоны считается UTC) и Unix epoch.
Единица epoch определяется по порядку величины: с, мс, мкс или нс.
Неправдоподобные значения (нулевая дата, раньше 2000 года, задержка больше часа
или меньше −1 минуты) в задержку не попадают, а идут в
`wsshared_bad_timestamps_total`: одна битая дата иначе отравила бы весь p95.

**Поиск ключа без `path`** находит **первое** (или последнее при `fromEnd`)
вхождение ключа на любой глубине, совпадения в строковых значениях
пропускаются. Если ключ встречается в нескольких местах, нужен `path`.

### `autoReply` и `heartbeat` — keep-alive в Go

```js
autoReply: [
    { match: { type: 'ping' }, reply: { type: 'pong' } },     // ответить и скрыть
    { match: { type: 'ack' }, swallow: false },               // только посчитать
    { match: 'heartbeat', reply: 'hb', raw: true },           // строка — сырые байты
],
heartbeat: { everyMs: 15000, send: { op: 'ping' } },          // свой пинг
heartbeat: { everyMs: 15000, control: true },                // WS ping control frame
```

Срабатывает первое подходящее правило. По умолчанию `swallow: true`: сообщение
не попадает ни в очередь, ни в `track`/`buffers`, ни в метрики задержки, только
в `autoReplied`. WS-уровневые ping/pong (control frames) обрабатываются всегда
и считаются в `stats().controlPings`.

### `onOpen` — что отправить сразу после коннекта

```js
onOpen: [
    { op: 'auth', token },                        // payload: через codec
    '{"protocol":"json","version":1}\x1e',        // строка: через codec
    { send: 'HELLO', raw: true, binary: false },  // явная форма: мимо codec
]
```

Отправляется при каждом подключении, в том числе после реконнекта. Объект с
ключом `send` считается явной формой. Payload, у которого есть собственное поле
`send`, заворачивайте: `{ send: { send: ... } }`.

### `reconnect`

```js
reconnect: true
reconnect: { maxAttempts: 5, minDelayMs: 500, maxDelayMs: 5000 }   // -1 = без лимита
```

Срабатывает только при неожиданном обрыве, `close()` никогда не реконнектит.
Backoff экспоненциальный с джиттером: без джиттера рестарт сервера заставил бы
все VU переподключиться в одну миллисекунду.

После реконнекта: заново `onOpen`, затем всё, что отправлялось с
`{ replay: true }`. Счётчики, `track`, `buffers` и непропушенные сэмплы
переживают реконнект.

### `track` — последнее значение

```js
track: [
    { name: 'bid', path: 'data.bid', keyBy: 'data.symbol', when: { type: 'quote' } },
    { name: 'bid', path: 'snapshot.bid', keyBy: 'snapshot.symbol' },  // то же имя: тот же слот
    { name: 'px', paths: ['data.px', 'result.px'] },                    // первый найденный
    { name: 'lastId', regex: '"id":"(T-\\d+)"' },
]
```

| Опция | Смысл |
|---|---|
| `name` | имя для `tracked(name)`; несколько спек с одним именем пишут в один слот |
| `path` / `paths` | jsonPath, из списка берётся первый найденный |
| `regex` / `group` | альтернатива пути, значение строкой |
| `when` | матчер: какие сообщения учитывать |
| `keyBy` | jsonPath к ключу: своё значение на каждый ключ |

### `buffers` — последние N сообщений

```js
buffers: [
    { name: 'trades', size: 50, when: { type: 'trade' }, keepBody: true },
    { name: 'errors', size: 10, when: { path: 'data.status', equals: 'rejected' } },
]
```

Кольцо фиксированного размера: VU, который не заглядывает в буфер, тратит
ограниченную память, а не растущую очередь. Буферы очередь **не** потребляют:
сообщение может лежать в буфере и одновременно прийти через `next()`.

### `tags`, `metrics`

`tags` вешаются на все метрики соединения. `metrics` выключает группы:
`{ enabled, delay, counters, parse, byType, session, readGap }`. По умолчанию
включено всё, кроме `readGap`.

---

## Матчеры

Один формат для `collect`, `waitFor`, `autoReply.match`, `track.when`,
`buffers.when`, `forgetReplay` и `ev.test`. Все указанные условия
объединяются через И.

```js
{ type: 'quote' }                                   // ev.type (нужен typePath)
{ type: ['quote', 'trade'] }                        // один из
{ path: 'data.symbol', equals: 'EURUSD' }           // значение по jsonPath
{ path: 'data.qty', oneOf: [1, 2, 3] }
{ path: 'data.error', exists: false }
{ path: 'channel', regex: '^quotes\\.' }            // регулярка по значению
{ field: 'code', equals: 0 }                        // по объявленному полю
{ contains: '"status":"filled"' }                   // подстрока тела, самое дешёвое
{ regex: '"qty":\\s*[5-9]\\d' }                     // регулярка по телу
{ any: [{ type: 'trade' }, { field: 'code', equals: 404 }] }   // ИЛИ
{ contains: 'heartbeat', not: true }                // отрицание
'"op":"ack"'                                        // строка = { contains: ... }
```

Сравнение **нестрогое**: `'0'` равно `0`. Поля всегда строки, а в скрипте
удобнее писать числа.

> **`keepBody` и матчеры.** `contains`, `regex` по телу и `path` читают тело.
> В read-loop (`autoReply`/`track`/`buffers`) тело есть всегда. А в
> `collect`/`waitFor` события берутся из очереди, где тело лежит только при
> `keepBody: true`. Без него такой матчер бросит понятную ошибку. Решение —
> `keepBody: true` либо матчинг по `type`/`field`: они тела не требуют.

---

## Методы `Conn`

| Метод | Возвращает | Что делает |
|---|---|---|
| `send(data, opts?)` | — | отправить. `data`: строка, объект (→ JSON), `ArrayBuffer`/`Uint8Array`. `opts`: `{ replay, raw, binary }` |
| `next(timeoutMs)` | событие \| `null` | одно событие. `0` — только из очереди, без ожидания |
| `run(seconds)` | сводка | крутить N секунд целиком в Go. `{ events, errors, reconnects, lastError?, lastDropError?, connectionError? }` |
| `collect(n, timeoutMs, match?)` | событие[] | до N подходящих событий за общий таймаут; одно пробуждение JS |
| `waitFor(match, timeoutMs)` | событие \| `null` | первое подходящее |
| `drain(max?)` | событие[] | всё, что уже в очереди, без ожидания |
| `tracked(name, key?)` | значение \| `null` | последнее значение (без ключа — самое свежее) |
| `trackedInfo(name, key?)` | объект | `{ found, value, text, key, type, updates, ageMs, misses }` |
| `trackedAll(name)` | `{ key: value }` | все ключи |
| `buffer(name)` | событие[] | содержимое кольца, старые первыми, без очистки |
| `takeBuffer(name)` | событие[] | то же и очистить |
| `bufferInfo(name)` | объект | `{ count, size, total, overwritten }` |
| `replay()` / `clearReplay()` | | список повторяемых сообщений / очистить |
| `forgetReplay(match)` | число | убрать из replay подходящие (напр. отвергнутую подписку) |
| `stats()` | объект | счётчики, сквозные через реконнекты |
| `close()` | объект | закрыть и дослать метрики |

`collect` и `waitFor` **потребляют** очередь: неподходящие события выбрасываются.
При этом они уже посчитаны в метриках и разложены по `track`/`buffers`.

**`ageMs` и `misses` в `trackedInfo`.** `tracked()` не отличает «значение
свежее» от «ничего не приходило минуту». Если решение зависит от свежести,
смотрите `ageMs`. `misses` растёт, когда `when` совпал, а путь не нашёлся:
так видна опечатка в пути.

### Событие

```js
{
    seq: 42,                    // сквозной номер сообщения в соединении
    type: 'quote',              // при typePath
    binary: true,               // тип WS-кадра
    size: 19591,                // размер сообщения после декодирования
    wireBytes: 4141,            // размер кадра на проводе (у первого сообщения кадра)
    decodedBytes: 19591,        // кадр после распаковки (у первого сообщения кадра)
    receivedAt: 1789725600123.4,// мс epoch, момент чтения из сокета
    error: '...',               // ошибка декодирования (strict)
    fields: { ts: '2026-...', symbol: 'EURUSD' },
    delays: { ts: 0.42 },       // мс, для asTimestamp

    // при keepBody (или keepBody буфера) — МЕТОДЫ: не вызвал — не заплатил
    bodySize: 19591,
    body(),                     // строка
    bytes(),                    // ArrayBuffer
    json(),                     // JSON.parse(body())
    jsonPath('data.levels[-1].px'),   // одно значение, разбор в Go
    match('"id":"(T-\\d+)"', 1),      // первое совпадение, строка | null
    matchAll('"px":([\\d.]+)'),       // все совпадения
    test({ path: 'data.qty', oneOf: [1, 2] }),
}
```

### jsonPath

Синтаксис намеренно маленький и работает по байтам, без построения дерева:

```
data.levels.3.px      data.levels[3].px      $.data.levels[3].px
data.levels[-1].px    // с конца
data['key.with.dots'] // ключ в кавычках
```

Нет wildcard, фильтров и рекурсивного спуска. Для них нужен парсер с деревом,
то есть ровно та стоимость, от которой ушли. Возвращает строку, число,
`true`/`false` или `null`. Объект или массив приходит JSON-текстом.
Ненайденный путь даёт `null`.

### Регулярки

Go `regexp` — это RE2: линейное время, без катастрофического бэктрекинга,
поэтому безопасно гонять на каждом сообщении. Синтаксис RE2 не поддерживает
lookahead и backreference. Скомпилированные шаблоны кешируются.

---

## Утилиты модуля (без сокета)

Та же машинерия для HTTP-ответов и захваченных кадров:

```js
ws.jsonPath(res.body, '$.result.items[-1].id')
ws.regex(res.body, '"token":"([^"]+)"')
ws.regexAll(res.body, '"id":(\\d+)', 1, 100)

const c = ws.codec({ compression: 'deflate', lengthPrefix: 'varint', delimiter: '\x1e' });
const f = c.encode({ op: 'ping' });          // { bytes: ArrayBuffer, size, base64 }
c.decode(f.bytes);                           // [{ body, size, wireBytes, decodedBytes, fallback }]
c.decodeBase64('M6pWSs5I...');               // кадр, скопированный из DevTools
```

---

## Метрики

| Метрика | Тип | Что |
|---|---|---|
| `wsshared_connect_duration_ms` | Trend | установка соединения |
| `wsshared_connect_success_rate` | Rate | доля успешных коннектов |
| `wsshared_session_duration_ms` | Trend | длительность сессии |
| `wsshared_message_delay_ms` | Trend | задержка по `asTimestamp`, тег `field` |
| `wsshared_bad_timestamps_total` | Counter | отброшенные таймштампы, тег `field` |
| `wsshared_parse_duration_ms` | Trend | работа генератора: декод, поля, матчеры, track, буферы |
| `wsshared_messages_received` / `_sent` | Counter | сообщения |
| `wsshared_wire_bytes_received` | Counter | байты на проводе |
| `wsshared_decoded_bytes_received` | Counter | после распаковки |
| `wsshared_bytes_sent` | Counter | исходящие байты |
| `wsshared_messages_by_type` | Counter | тег `messageType` (при `typePath`) |
| `wsshared_auto_replied_total` | Counter | обработано `autoReply` |
| `wsshared_events_dropped_total` | Counter | выброшено политикой `overflow` |
| `wsshared_decode_errors_total` | Counter | ошибки декодирования |
| `wsshared_reconnects_total` / `_success_rate` / `_duration_ms` / `_exhausted_total` | | реконнекты |
| `wsshared_read_gap_ms` | Trend | диагностика перегрузки генератора, по умолчанию выключена |

Префикс `wsshared_` выбран, чтобы не конфликтовать с самописными `ws_*`
метриками скриптов: k6 не даёт зарегистрировать одно имя с разными типами.
Через Prometheus remote-write k6 добавит свой префикс: `k6_wsshared_...`.

**Когда метрики уходят.** Сэмплы задержки и типов создаются в read-loop в
момент приёма, с настоящим временем приёма. Поэтому сообщение измерено, даже
если скрипт его не прочитал: выброшено `collect`-ом, `overflow`-ом, или VU
пользуется только `tracked()`. В k6 сэмплы пушит VU: на `next`/`run`/`collect`/
`tracked`/`buffer`/`stats`/`close` (не чаще раза в 100 мс, `close` — всегда).
Если VU долго не трогает соединение, сэмплы копятся (не больше 65536, сверх
того считаются в `stats().samplesDropped`) и уходят пачкой со своими
таймштампами.

**Коэффициент сжатия** = `decodedBytesIn / wireBytesIn`.

**`read_gap_ms`** — время между «горутина готова читать» и «следующий кадр
получен», только для кадров, которые уже лежали в буфере ядра (разрыв не
больше 20 мс). Если растёт с числом VU, генератор не успевает читать, и в
`message_delay_ms` подмешана собственная задержка генератора.

---

## Примеры

Мок-сервер `examples/mockws` выдумывает поток котировок и сделок. Формат кадров
настраивается флагами, так что один мок проверяет все слои кодека.

```bash
go run ./examples/mockws                                           # plain JSON
go run ./examples/mockws -addr=127.0.0.1:8091 -compression=deflate -varint -delimiter=rs
go run ./examples/mockws -addr=127.0.0.1:8092 -drop-after=3s       # обрывы

./k6-wsshared.exe run examples/smoke.js
./k6-wsshared.exe run examples/collect.js
./k6-wsshared.exe run examples/track.js
./k6-wsshared.exe run --env WS_URL=ws://127.0.0.1:8091/ws examples/framing.js
./k6-wsshared.exe run --env WS_URL=ws://127.0.0.1:8092/ws examples/reconnect.js
```

| Пример | Про что |
|---|---|
| [`smoke.js`](examples/smoke.js) | минимальный нагрузочный цикл: onOpen, autoReply, run |
| [`collect.js`](examples/collect.js) | `waitFor`, `collect` с матчерами, `drain`, jsonPath/регулярки, утилиты |
| [`track.js`](examples/track.js) | VU занят другим: `track` + `buffers` + `queue: false` |
| [`framing.js`](examples/framing.js) | сжатый бинарный канал, standalone `codec` |
| [`reconnect.js`](examples/reconnect.js) | реконнект, replay, `forgetReplay` |
| [`hubs.js`](examples/hubs.js) | N хабов × несколько сокетов на VU, HTTP-триггер, сверка доставки через xk6-shared (мок `mockhub`) |

Флаги мока: `-addr`, `-every`, `-levels` (размер payload), `-compression`,
`-varint`, `-delimiter=nl|rs`, `-drop-after`, `-v`.

### Несколько хабов, HTTP-триггер, подсчёт доставки

Мок `examples/mockhub` держит N хабов (`ws://…/hub/{name}`). Сообщения в них
публикуются через HTTP: `POST /api/{hub}/publish {"topic":…}` отвечает, скольким
сокетам ушло сообщение, а `GET /api/stats` показывает, сколько сокетов сервер
видит на каждом хабе.

```bash
go run ./examples/mockhub                    # orders,prices,alerts на 127.0.0.1:8091; -v — лог каждого сокета
./k6-wsshared.exe run -q --log-format=raw examples/hubs.js
./k6-wsshared.exe run -q --log-format=raw -e USERS=100 -e PER_HUB=3 -e LOG=0 examples/hubs.js

go run ./examples/mockhub -addr=127.0.0.1:8092 -compression=deflate -varint -delimiter=rs
./k6-wsshared.exe run -q --log-format=raw -e BASE=http://127.0.0.1:8092 -e FRAMING=packed examples/hubs.js

# бинарные форматы: та же логика, те же jsonPath-вырезки
go run ./examples/mockhub -addr=127.0.0.1:8093 -format=msgpack
./k6-wsshared.exe run -q --log-format=raw -e BASE=http://127.0.0.1:8093 -e FORMAT=msgpack examples/hubs.js
go run ./examples/mockhub -addr=127.0.0.1:8094 -format=protobuf -compression=deflate -varint
./k6-wsshared.exe run -q --log-format=raw -e BASE=http://127.0.0.1:8094 -e FORMAT=protobuf -e FRAMING=packed examples/hubs.js
```

Схема для protobuf — [`examples/mockhub/hub.proto`](examples/mockhub/hub.proto).
Имена полей в ней совпадают с JSON-ключами, поэтому вырезки одинаковые во
всех трёх форматах.

[`hubs.js`](examples/hubs.js): `USERS` слушателей, у каждого `PER_HUB` сокетов
на каждый хаб.

1. Каждый сокет подписывается на `user-<VU>` и `all`.
2. Слушатель шлёт себе личное сообщение через HTTP в каждый хаб и находит его на
   каждом своём сокете по `msgId` (`waitFor` по `field`). В лог идут вырезки
   jsonPath/регулярок, а от одного VU ещё и целое сообщение.
3. На `+3s` отдельный VU-вещатель считает сокеты: сколько открыл k6 (через
   xk6-shared) и сколько видит сервер. Потом он рассылает `BCAST` сообщений в
   `all`. Слушатели забирают их через `collect(BCAST, …)`.
4. Слушатели показывают последнее значение (`track`) и последние N сообщений
   (`buffers`), затем закрывают сокеты.
5. `teardown()` сводит в таблицу по хабам: открыто k6 / сервером, закрыто,
   доставлено по словам сервера, получено сокетами, потеряно.

Переменные: `BASE`, `HUBS`, `USERS`, `PER_HUB`, `BCAST`, `FRAMING=packed`,
`FORMAT=json|msgpack|protobuf`, `LOG=0|1|2`. `0` — только итог, `1` —
процесс и целые сообщения от одного VU, `2` — целые сообщения от всех.

---

## Разработка

```bash
go vet ./...
go test ./...                       # весь пакет идёт под goleak (TestMain)
go test -race -count=5 ./...        # нужен cgo + gcc (на Windows: портативный MinGW-w64)
go test -run=NONE -fuzz='^FuzzJSONPath$' -fuzztime=40s ./   # + FuzzCodecDecode,
                                    #   FuzzCodecRoundTrip, FuzzCodecFormatRoundTrip, FuzzExtractAndMatch
go test -run=NONE -fuzz='^FuzzMsgPackToJSON$' -fuzztime=40s ./binfmt   # + FuzzMsgPackRoundTrip,
                                    #   FuzzProtobufToJSON
go test -run=NONE -bench=. ./...
```

Уровни тестов: Go-ядро (`core_test.go`, `client_test.go`), JS-уровень через
k6 `modulestest` (`js_test.go`: ошибки конфига, весь API, метрики, реконнект,
бинарный канал), фаззинг байтовых парсеров (`fuzz_test.go`,
`binfmt/binfmt_test.go`), goleak на весь пакет (`main_test.go`).

Пакет [`binfmt`](binfmt) — форматы без зависимости от k6. Его импортирует и
мок. Тесты MessagePack написаны на байтах из спецификации, а не на выводе
своего же энкодера. Для protobuf проверяются оба пути загрузки схемы
(`.proto` и descriptor set), импорты, кеш и конкурентная загрузка под `-race`.

Тесты покрывают: jsonPath (скобки, отрицательные индексы, ключи с точками,
unicode-escape), round-trip всех комбинаций кодека, несколько сообщений в
кадре, fallback и strict, многобайтовый VarInt, поля (ключ, путь, регулярка,
ключ-как-значение), таймштампы (ISO и все виды epoch, отбраковка), матчеры
(все условия, any/not, нестрогое сравнение), track (paths, keyBy, regex, when,
misses), кольцевые буферы, onOpen, autoReply с проглатыванием, heartbeat
(app и control), все политики overflow, разблокировку read-loop на close,
конкурентный close без утечки горутин, джиттер backoff.

## Ограничения

- Реконнект не догоняет пропущенное: соединение поднимается заново, `onOpen` и
  replay переотправляются, сообщения за время простоя теряются.
- Заголовки и токен на реконнекте те же, что при `connect()`. Если токен истёк,
  поднимайте соединение новой итерацией со свежим токеном (например, из пула
  `xk6-shared`).
- Тело в событиях из очереди есть только при `keepBody`. Тело не копируется
  (zero-copy), но удерживается в памяти, пока событие лежит в очереди или
  буфере.
- `fields` всегда строки, `jsonPath()` — типизированные значения. Это сделано
  сознательно: поля — дешёвый транспорт, а не декодер.
- Бинарные форматы: MessagePack без схемы и protobuf со схемой. Protobuf без
  схемы не поддерживается: без неё нельзя отличить вложенное сообщение от
  строки или байтов. CBOR, Avro и прочие добавляются как ещё один
  `binfmt.Format`.
