# xk6-shared — полный справочник (для агента / разработчика)

Расширение k6, дающее **разделяемое изменяемое состояние между VU** внутри
одного `k6 run`. Импорт в JS: `import shared from 'k6/x/shared'`.

Этот документ — исчерпывающий: как устроено внутри, все методы с сигнатурами и
поведением, все подводные камни, готовые кейсы, правила использования. Читать
целиком перед тем, как писать код с shared.

---

## 0. TL;DR (самое важное за 30 секунд)

- Состояние живёт в **Go-памяти процесса**, ОДНА копия, видно ВСЕМ VU. Переживает
  границу `setup()` → VU (обычные JS-переменные — нет).
- Область — **один процесс `k6 run`** (один генератор). Между машинами НЕ шарится
  → для распределённого состояния нужен Redis.
- **Два железных правила:**
  1. Все читающие методы (`get`/`at`/`random`/`getPath`/`tableRow`/`tableRandom`/
     `tableField`/`snapshot`/`unflatten`) возвращают КОПИЮ композитов — мутировать
     результат БЕЗОПАСНО всегда (стор не тронется, краш невозможен). Записать в
     стор — только через `set*`/`setPath`/`update*`. Скаляры копируются бесплатно.
  2. **Не гонять большие данные через границу целиком** (`snapshot`, `get` всего
     пула). Читать по элементу (`random`/`at`/`getPath`) — тогда быстро при любом
     размере.
- **Никаких busy-wait/спин-циклов вокруг shared** на многих VU — это кладёт машину
  (см. раздел «Подводные камни», пункт про спин).

---

## 1. Как устроено внутри (низкий уровень)

**Процесс и VU.** Один `k6 run` — один ОС-процесс, одна Go-runtime. Каждый VU —
это goroutine + свой изолированный JS-движок (sobek/goja). JS-кучи VU
**изолированы**: глобальная JS-переменная одного VU не видна другому.

**Где живёт состояние.** Расширение держит один package-level `store` в Go
(создан через `sync.Once`). Каждый домен данных **шардирован на 16 полос
(lock striping)** по хешу имени ключа — как `ConcurrentHashMap`. Записи в
РАЗНЫЕ ключи/списки попадают в разные полосы → разные мьютексы → не мешают друг
другу:
```
store {
    valShards  [16]valShard    // скаляры/объекты; valShard{ mu; m map[string]*valEntry }
    listShards [16]listShard   // пулы/списки + индекс уникальности pushUnique
    treeShards [16]treeShard   // flatten-деревья (путь -> лист)
    counters   sync.Map        // атомарные счётчики (name -> *int64), lock-free
}
valEntry  { val interface{}; version uint64 }   // ВЕРСИЯ НА КАЖДЫЙ КЛЮЧ (для update CAS)
treeEntry { m map[string]…;  version uint64 }    // версия на каждое дерево
```
Ключевое: `version` — **на каждый ключ/дерево** (в `valEntry`/`treeEntry`), не
одна на весь стор. Поэтому `update('a')` ретраится только если изменился именно
`a`, а не при любой записи куда угодно. `shardOf(key)` = FNV-1a хеш & 15.

Все VU (goroutine) держат указатель на ЭТОТ ЖЕ store. Отсюда шаринг — Go-heap
общий для всех goroutine процесса, в отличие от изолированных JS-куч.

**Как происходит вызок.** `shared.get(...)` из JS → sobek вызывает Go-метод
`(*ModuleInstance).Get(...)` **в той же goroutine** этого VU. Это обычный вызов
Go-функции — НЕ поток, НЕ сеть, НЕ IPC.

**Синхронизация.**
- чтения (`get`/`at`/`getPath`/…) берут `RLock` СВОЕЙ полосы — идут ПАРАЛЛЕЛЬНО;
- записи (`set`/`push`/`setAt`/…) берут `Lock` СВОЕЙ полосы — эксклюзивно, но
  только относительно других записей в ту же полосу (тот же хеш ключа);
- счётчики (`incr`/`add`) — lock-free `atomic.AddInt64` (аппаратная инструкция);
- `random` использует `math/rand/v2` (lock-free, без общего RNG-мьютекса).
Каждый метод берёт РОВНО ОДИН лок одной полосы (нет вложенных локов → нет
дедлоков). Мьютекс держится наносекунды.

**Contention и его потолок.** Striping снимает конкуренцию когда ключи РАЗНЫЕ.
НО если 3000 VU пишут в ОДНО имя (`push('q')`, `update('acc')`) — все хешируются
в одну полосу → один лок → они снова конкурируют (потолок физический). Для этого
случая есть `*Shard`-методы (см. раздел 2b) — они шардируют по номеру VU внутри
себя, снимая contention полностью, без изменений в логике теста.

**Граница Go ↔ JS (ключ к пониманию стоимости).** Возврат Go-значения sobek
конвертирует в JS:
- `string`/`number`/`bool`/`null` — по значению (тривиально);
- `map`/`slice` (объект/массив) — оборачивается **ЖИВОЙ ССЫЛКОЙ** на Go-объект,
  НЕ глубокой копией. Поэтому `get` большого объекта дёшев (данные не копируются),
  НО мутация полей этой ссылки из JS пишет прямо в общий Go-объект, минуя мьютекс
  (см. подводные камни).

---

## 2. Импорт и жизненный цикл

```js
import shared from 'k6/x/shared';

export function setup() {
    shared.flushAll();                 // ВСЕГДА очищай стор в начале
    shared.pushMany('pool', [...]);    // готовь общие данные здесь (один раз, до VU)
}
export default function () {
    const x = shared.random('pool');   // VU читают/меняют
}
```
`setup()` выполняется ОДИН раз ДО всех VU. Данные, положенные в shared в setup,
видны всем VU (это работает именно потому, что состояние в Go, а не в JS-isolate).

---

## 3. Поддерживаемые типы значений

`string`, `number` (int/float, до 2^53), `boolean`, `null`, массивы, объекты
(вложенные, массивы объектов).

Нюансы (как в JSON):
- **массивы** — порядок элементов СОХРАНЯЕТСЯ;
- **объекты** — порядок КЛЮЧЕЙ НЕ гарантируется (Go map). Обращаться по имени поля,
  не сравнивать объекты через `JSON.stringify`;
- `undefined` → сохраняется как `null`;
- нельзя: функции, `Date`, `Map`/`Set`, циклические ссылки.

---

## 4. ВСЕ методы (сигнатура · поведение · когда)

### 4.1 Скалярные переменные

**`set(key, value)` → void**
Записать/перезаписать. Значение на входе КОПИРУЕТСЯ (мутация исходного JS-объекта
после set не меняет стор). Потокобезопасно (под Lock).
```js
shared.set('cfg', { wallet: 'EUR' });
```

**`get(key)` → SharedValue | null**
Прочитать; `null` если нет. Для объектов/массивов возвращает ЖИВУЮ ССЫЛКУ (не
копию) — только читать.
```js
const cfg = shared.get('cfg');       // читать ок
// cfg.wallet = 'USD';               // ❌ НЕЛЬЗЯ — гонка
```

**`has(key)` → boolean** — есть ли ключ.
**`delete(key)` → void** — удалить.

**`setIfAbsent(key, value)` → boolean**
Записать ТОЛЬКО если ключа нет. `true` если записал. Атомарно. Паттерн «первый
VU выигрывает».
```js
if (shared.setIfAbsent('inited', true)) { /* один раз на процесс */ }
```

**`compareAndSwap(key, expected, next)` → boolean**
Установить `next` только если текущее == `expected`. `true` при успехе. Для
выбора лидера / латчей. (absent трактуется как совпадение только если expected===null)
```js
if (shared.compareAndSwap('leader', null, __VU)) { /* я лидер */ }
```

**`update(key, fn)` → SharedValue**
Атомарный read-modify-write. `fn(current)` ДОЛЖНА вернуть новое значение.
Безопасно даже когда много VU меняют ОДИН ключ (нет потерянных обновлений).
Внутри: deepCopy значения → fn работает на изолированной копии (вне мьютекса) →
коммит под мьютексом с проверкой версии; при конкуренции fn может вызваться
ПОВТОРНО (должна быть без побочных эффектов).
```js
shared.update('cfg', c => { c.limit += 1; return c; });
```

### 4.2 Атомарные счётчики (lock-free)

**`incr(name)` → number** — +1, возвращает новое.
**`add(name, delta)` → number** — +delta (можно отрицательный), возвращает новое.
**`count(name)` → number** — текущее (0 если не трогали).
**`resetCounter(name)` → void** — сброс в 0.
```js
const n = shared.incr('ops');   // атомарно, без потерь на любом числе VU
```
Всегда предпочитать `incr` над `set(k, get(k)+1)` — второй теряет инкременты.

### 4.3 Пулы / списки

**`push(name, value)` → number** — добавить в конец, вернуть длину.
**`pushUnique(name, value)` → boolean** — добавить если нет (по строковой форме),
`true` если добавлено. O(1) через индекс. `42` и `"42"` считаются одинаковыми.
**`pushMany(name, values[])` → number** — добавить пачкой (одна блокировка), длина.
**`size(name)` → number** — длина.
**`at(name, i)` → SharedValue | null** — элемент по индексу (отрицательный — с
конца), null вне диапазона.
**`setAt(name, i, value)` → boolean** — ЗАМЕНИТЬ элемент по индексу
(потокобезопасно), true если в диапазоне.
**`random(name)` → SharedValue | null** — случайный элемент. ГОРЯЧИЙ ПУТЬ:
копирует ОДИН элемент, не весь пул. null если пусто.
**`randomN(name, n)` → SharedValue[]** — до n РАЗНЫХ случайных (без повторов).
**`pop(name)` → SharedValue | null** — снять последний (LIFO).
**`shift(name)` → SharedValue | null** — снять первый (FIFO / очередь).
**`listHas(name, value)` → boolean** — есть ли в индексе уникальности (только для
pushUnique-списков).
**`snapshot(name)` → SharedValue[]** — КОПИЯ всего списка. Дорого — не в hot-loop.
**`clearList(name)` → void** — очистить список и его индекс.

Выбор:
| Нужно | Метод |
|---|---|
| пул, VU берут случайный | `pushMany` (setup) + `random` (VU) |
| каждый VU берёт УНИКАЛЬНУЮ запись | `shift` (никто не получит дважды) |
| producer/consumer | `push` + `shift` |
| дедуп при наполнении | `pushUnique` |
| заменить элемент | `setAt` |
| обойти по индексу | `size` + `at` |

### 4.3b Авто-шардированные методы — когда МНОГО VU пишут в ОДНО имя ⚡

Та же семантика, что у `push`/`pushUnique`/`shift`/`random`/`update`, но запись
раскидывается по 16 внутренним под-ключам (по номеру VU). Тысячи писателей НЕ
дерутся за один лок. **В тесте ничего добавлять не надо** — метод шардирует сам
(никаких `:'+__VU%16` руками). Читатели собирают всё обратно по всем шардам.

Очередь / список под массовой записью:
**`pushShard(name, value)` → number** — как `push`, но контеншен-фри между VU. Длина шарда.
**`pushUniqueShard(name, value)` → boolean** — как `pushUnique`; уникальность В ПРЕДЕЛАХ шарда (не глобально).
**`sizeShard(name)` → number** — СУММАРНАЯ длина по всем шардам.
**`shiftAny(name)` → SharedValue | null** — снять из любого непустого (FIFO в шарде, round-robin между). null если вся очередь пуста.
**`popAny(name)` → SharedValue | null** — LIFO из любого непустого шарда.
**`randomShard(name)` → SharedValue | null** — равномерный случайный по всему логическому списку (взвешенно по размеру шардов).
**`snapshotShard(name)` → SharedValue[]** — КОПИЯ всего (все шарды). setup/debug, не hot-loop.
**`clearShard(name)` → void** — очистить все шарды.

Аккумулятор под массовой записью (каждый VU в свой шард → лок-фри):
**`updateShard(name, fn)` → SharedValue** — атомарный RMW своего шарда. Комбинировать через `readShard`.
**`readShard(name, mergeFn, seedFn)` → SharedValue** — слить 16 шардов: `seedFn()` даёт начальное, `mergeFn(acc, part)` складывает каждый шард.

```js
// producer (много VU) -> consumer, без контеншена:
shared.pushShard('cashout', { userToken, betId });
const ctx = shared.shiftAny('cashout');            // consumer, из любого шарда

// горячий счётчик/аккумулятор (все VU в одно имя):
shared.updateShard('stats', s => { s = s || {n:0}; s.n++; return s; });
const total = shared.readShard('stats', (a,p)=>({n:a.n+(p?p.n:0)}), ()=>({n:0}));
```

**Плата за отсутствие лока:** `shiftAny` — НЕ строгий глобальный FIFO (FIFO внутри
шарда, между шардами round-robin); `pushUniqueShard` дедупит в пределах шарда, не
глобально. Нужен строгий FIFO / глобальная уникальность — обычные `shift`/
`pushUnique` (ценой контеншена). Замер (3000 VU): `pushShard` ~19µs против `push`
в 1 список 911µs (×48); `updateShard` ~4.3ms против `update` в 1 ключ 17ms (×4).

Когда что: **разные ключи** → обычные методы (striping и так разносит). **Одно
имя всеми VU в hot-loop** → `*Shard`. **Точное глобальное множество/строгий FIFO**
→ обычные (contention как цена корректности).

### 4.3c Колоночные таблицы (SoA) — память на пулах однотипных объектов

Для БОЛЬШОГО пула ПЛОСКИХ ОДНОТИПНЫХ записей (event-пул, user rows): хранит
колоночно (один типизированный слайс на поле), а не N отдельных map. **~4.6×
меньше памяти** (замер: 500k строк 179МБ→39МБ). Схема выводится из первого
объекта и растёт сама. Пушишь как в список — ничего объявлять не надо.

**`tablePush(name, obj)` → number** — добавить строку, вернуть число строк.
Паника (ловимая в JS), если `obj` не объект.
**`tablePushMany(name, objs[])` → number** — пачкой; не-объекты пропускаются (лог).
**`tableRow(name, i)` → object|null** — строка по индексу (КОПИЯ), отрицательный — с конца.
**`tableRandom(name)` → object|null** — случайная строка (КОПИЯ). Реконструирует ОДНУ строку.
**`tableField(name, i, field)` → value|null** — ОДНО поле без сборки всей строки (дёшево, 24ns).
**`tableSize(name)` → number** — число строк.
**`tableClear(name)` → void** — очистить.

Когда: **плоские однотипные объекты + большой пул** → таблица (память ×4.6). Для
вложенных/разношёрстных → обычный `push`/`pushMany` (вложенное поле в таблице
падает в boxed-колонку — работает, но без экономии). Отсутствующее поле → дефолт
(`0`/`""`/`false`); несуществующее в `tableField` → `null`. Разный тип поля в
разных объектах → колонка промотируется в boxed (данные не теряются).

### 4.4 Дерево / вложенный JSON (flatten)

**`flatten(name, obj)` → number**
Разложить вложенный объект в пути `a.b.c` → листья. Возвращает число листьев.
Массивы: индекс = сегмент пути (`bets.0.stake`). Звать ОДИН раз (setup).
```js
shared.flatten('cfg', { wallet:{ code:'EUR', limit:500 }, bets:[{stake:10}] });
// пути: wallet.code, wallet.limit, bets.0.stake
```
**`getPath(name, path)` → SharedValue | null** — значение листа по пути. O(1),
копирует один лист. Любая глубина, индексы массива.
```js
shared.getPath('cfg', 'wallet.limit');    // 500
shared.getPath('cfg', 'bets.0.stake');    // 10
```
**`setPath(name, path, value)` → boolean** — записать лист (потокобезопасно).
**`updatePath(name, path, fn)` → SharedValue** — атомарный read-modify-write листа
(та же модель, что `update`).
**`hasPath(name, path)` → boolean** — есть ли путь.
**`deletePath(name, path)` → void** — удалить лист.
**`treeSize(name)` → number** — число листьев.
**`pathKeys(name, prefix)` → string[]** — все пути (с префиксом — только поддерево).
O(K). Не в hot-loop.
**`pathSearch(name, substr)` → string[]** — все пути, СОДЕРЖАЩИЕ подстроку.
**O(N) полный перебор — ТОЛЬКО в setup, НЕ в hot-loop.**
**`unflatten(name)` → SharedValue** — собрать объект обратно (массивы
восстанавливаются). Копирует всё — редко (teardown/debug).
**`treeClear(name)` → void** — удалить всё дерево целиком (аналог clearList/
tableClear для деревьев; deletePath удаляет только один лист).

Как забрать «вид» вложенностей списком:
```js
const stakePaths = shared.pathSearch('cfg', '.stake');           // пути
const stakes = stakePaths.map(p => shared.getPath('cfg', p));    // значения
```

### 4.5 Загрузчики из Go (парсинг файла в Go, не в JS)

**`loadCsv(name, csvText)` → number** — CSV → пул объектов-строк по заголовку.
**`loadJson(name, jsonText)` → number** — JSON → flatten-дерево (число листьев).
**`loadJsonPool(name, jsonText)` → number** — JSON-МАССИВ → пул записей (размер).
```js
shared.loadCsv('rows', open('big.csv'));
shared.loadJson('cfg', open('config.json'));
shared.loadJsonPool('events', open('events.json'));
```
Смысл: большой файл парсится ОДИН раз в Go, одна копия — против `open()+JSON.parse`
в каждом VU (дублирование на все isolate).

### 4.6 Служебное

**`flushAll()` → void** — очистить ВЕСЬ стор. ВСЕГДА в начале `setup()`.
**`keys()` → string[]** — имена скалярных переменных (debug).
**`lists()` → string[]** — имена списков (debug).

---

## 5. ПОДВОДНЫЕ КАМНИ (обязательно к прочтению)

### 5.1 Все чтения возвращают КОПИЮ — мутировать результат безопасно
`get`/`at`/`random`/`getPath`/`tableRow`/`tableRandom`/`tableField`/`snapshot`/
`unflatten` возвращают **КОПИЮ** композитных значений — мутировать результат
безопасно ВСЕГДА, стор не изменится, краш невозможен. (deepCopy на выходе, под
локом. Раньше отдавалась живая ссылка → мутация из нескольких VU роняла процесс
`concurrent map writes` — исправлено. Скаляры копируются бесплатно.)
```js
const c = shared.get('cfg'); c.limit = 9;   // ✅ меняет ЛОКАЛЬНУЮ копию
shared.set('cfg', c);                        // чтобы ЗАПИСАТЬ — через метод (замена под локом)
shared.update('cfg', c => { c.limit++; return c; });  // или атомарно
```
> Не рассчитывай, что мутация результата `get` «запишется» — это копия. Единственный
> способ изменить общий объект: `get`→изменить копию→`set`, либо `update`/`setPath`.
> (Методов «живой ссылки» `*Single` больше НЕТ — они создавали фатальную гонку даже
> при «1 пишет, N читают», поэтому убраны.)

### 5.2 НИКОГДА не делать busy-wait/спин вокруг shared на многих VU ⚠️ КРИТИЧНО
Спин-цикл опроса (`for(...) { if(shared.get(x)) break; }`) на тысячах VU
раскручивает 100% CPU + contention на мьютексе → **машина уходит в своп и
умирает** (проверено на 11700 VU — положило генератор).
```js
// ❌ СМЕРТЬ машины на многих VU:
while (!shared.get('ready')) { /* спин */ }
```
Координация должна быть БЕЗ ожидания: клади готовые данные в shared в `setup()`
(он до VU и один раз), а VU просто читают. Если нужен «первый готовит» — данные
всё равно готовь в setup, не в первой итерации VU со спином остальных.

### 5.3 Область — один процесс
shared НЕ виден между генераторами/машинами. Распределённый прогон общего
состояния через shared не получит. Для этого — Redis (или гибрид: Redis для
межгенераторной координации в setup → сразу в shared → тест читает из памяти).

### 5.4 Порядок ключей объектов не гарантируется
`unflatten`/`get` объекта может вернуть ключи в другом порядке (Go map). Значения
верны. Не полагаться на порядок ключей, не сравнивать объекты строкой. Массивы —
порядок сохраняют.

### 5.5 Большие данные через границу целиком — дорого
`get`/`snapshot` всего большого пула/объекта копирует всё в isolate. На 10k+ VU —
память/CPU. Читать по элементу (`random`/`at`/`getPath`). Класть большое —
`pushMany`/`loadJsonPool`/`flatten` один раз в setup.

### 5.6 pathSearch / pathKeys / unflatten — O(N)
Полный обход дерева. Только в setup. В hot-loop — закэшировать результат (пути) в
пул и брать `random`/`at`.

### 5.7 update: fn может вызваться повторно
При конкуренции оптимистичный CAS повторяет цикл → `fn` может выполниться >1 раза.
`fn` должна ТОЛЬКО вычислять новое значение, без сайд-эффектов (не incr внутри,
не HTTP и т.п.).

### 5.8 undefined → null; типы см. раздел 3.

---

## 6. ГОТОВЫЕ КЕЙСЫ (копировать и адаптировать)

### Кейс 1 — общий пул событий, VU берут случайное
```js
export function setup() {
    shared.flushAll();
    shared.pushMany('events', [101, 202, 303, 404]);   // один раз
}
export default function () {
    const eventId = shared.random('events');           // hot-loop, ~µs
}
```

### Кейс 2 — каждый VU берёт УНИКАЛЬНУЮ запись (без повторов между VU)
```js
export function setup() { shared.flushAll(); shared.pushMany('queue', rows); }
export default function () {
    const row = shared.shift('queue');   // FIFO, атомарно, никто не получит дважды
    if (row === null) return;            // пул исчерпан
}
```

### Кейс 3 — общие счётчики
```js
export default function () {
    const total = shared.incr('requests');
    if (!ok) shared.incr('errors');
}
```

### Кейс 4 — «первый VU готовит данные» (без спина!)
Готовь в setup. Если данные приходят извне (HTTP/Redis) — тоже в setup:
```js
export function setup() {
    shared.flushAll();
    const data = fetchFromSomewhere();     // один раз, до VU
    shared.pushMany('pool', data);
}
export default function () { shared.random('pool'); }
```

### Кейс 5 — большой файл всем VU
```js
export function setup() {
    shared.flushAll();
    shared.loadCsv('users', open('users_200mb.csv'));  // парсинг в Go, одна копия
}
export default function () { const u = shared.random('users'); }  // ~µs, размер не важен
```

### Кейс 6 — вложенный json, доступ по полю
```js
export function setup() {
    shared.flushAll();
    shared.loadJson('cfg', open('config.json'));       // flatten
    // дорогой поиск ОДИН раз → кэш путей в пул
    shared.pushMany('idPaths', shared.pathSearch('cfg', '.eventId'));
}
export default function () {
    const path = shared.random('idPaths');             // дёшево
    const v = shared.getPath('cfg', path);             // O(1)
}
```

### Кейс 7 — атомарное изменение общего объекта
```js
export function setup() { shared.flushAll(); shared.set('stats', { placed:0 }); }
export default function () {
    shared.update('stats', s => { s.placed += 1; return s; });  // без потерь
}
// Если ОБЩИЙ объект бьют ТЫСЯЧИ VU в hot-loop — updateShard (лок-фри), а итог
// собрать через readShard:
//   shared.updateShard('stats', s => { s = s||{placed:0}; s.placed++; return s; });
//   const total = shared.readShard('stats', (a,p)=>({placed:a.placed+(p?p.placed:0)}), ()=>({placed:0}));
```

### Кейс 8 — producer/consumer между потоками
```js
export default function () {
    if (isProducer) shared.push('jobs', { id: shared.incr('jobId') });
    else { const job = shared.shift('jobs'); if (job) process(job); }
}
// Под МАССОВОЙ записью (много producer-VU в одну очередь) — pushShard/shiftAny
// (×48 throughput, порядок между шардами не строгий FIFO):
//   if (isProducer) shared.pushShard('jobs', job);
//   else { const job = shared.shiftAny('jobs'); if (job) process(job); }
```

### Кейс 9 — гибрид с Redis (распределённо, из реального проекта QA-5290)
Redis — только координация между генераторами в setup. Дальше всё из shared.
```js
export async function setup() {
    // ... получить пул событий, договориться между генераторами через Redis ...
    // СРАЗУ положить в shared — весь тест читает из памяти (переживает TTL redis):
    shared.pushMany('evpool:phase1', phase1Ids);
}
// VU читают shared.snapshot('evpool:phase1')/random — redis больше не трогают
```

---

## 7. Сборка бинаря (напоминание)

```bash
xk6 build v1.4.2 \
  --with github.com/altenar/xk6-shared=/path/to/xk6-shared \
  [... прочие расширения проекта ...] \
  --output ./k6
```
Windows: путь нативный (`pwd -W`), не `/c/...`. Расширение — чистый Go, cgo не
нужен. `go test -race` требует cgo (на Windows — gcc; в CI/Linux из коробки).

---

## 8. Производительность (замерено, Go ns/op)

Чистые Go-операции (наносекунды, без goja): count 9, has/size/treeSize 11-13,
getScalar 12, getPath 16, incr 31, set 26. Чтение СКАЛЯРА — бесплатно.

**RPS через k6+goja (1000 VU, реальный throughput — ЧТО важно для нагрузки, а не
Go-микробенч).** Тыс. операций/сек на один генератор, по размеру пула (число
элементов: 1k / 20k / 200k):

| Метод | 1k | 20k | 200k | | Метод | 1k | 20k | 200k |
|---|---|---|---|---|---|---|---|---|
| getScalar | 2894 | 2957 | 2339 | | count | 2758 | 2773 | 2187 |
| has | 2967 | 2939 | 2314 | | incr | 2711 | 2719 | 2151 |
| size | 2644 | 2593 | 2100 | | treeSize | 2228 | 2228 | 1732 |
| random (строки) | 2483 | 2430 | 1825 | | at | 2358 | 2316 | 1823 |
| getPath | 2118 | 2131 | 1619 | | hasPath | 2060 | 2111 | 1640 |
| get объекта | 1906 | 1943 | 1550 | | tableField | 1746 | 1382 | 1378 |
| push | 1896 | 1795 | 1321 | | pushShard | 1814 | 1445 | 1380 |
| random объекта | 1733 | 1588 | 1249 | | **randomN** | 1445 | 1457 | 1191 |
| tableRandom | 1490 | 1324 | 1029 | | tableRow | 1418 | 1213 | 1045 |
| shiftAny (queue) | 1139 | 1014 | 908 | | update (1 ключ) | 704 | 698 | 513 |
| updateShard | 867 | 771 | 702 | | readShard | 587 | 530 | 456 |
| **snapshot** (весь пул) | 326 | 89 | 71 | | **snapshotShard** | ~2 | ~1 | ~1 |

**Как читать:**
- **Почти всё O(1)** — RPS не зависит от размера пула (random/at/get/tableField/
  счётчики держат 1.3–2.3M даже на 200k; ~20% просадка 1k→200k это общий RAM).
- **`randomN` теперь O(n)** (rejection-sampling) — ~1.2–1.4M на любом размере
  (раньше обходил весь пул: 200k давал всего 7k RPS, ×170 медленнее).
- **`snapshot`/`snapshotShard` — O(N), копируют ВЕСЬ пул** → падают с размером.
  Только setup/teardown/debug, НЕ hot-loop. Нужен один элемент — `random`/`at`.
- **`update` в один ключ** — ниже (0.5M, CAS-ретраи). Массовая запись → `updateShard`.

Сравнение: redis localhost ~77µs/оп (~13k RPS 1 потоком) → shared ~×80 по RPS; vs
kv (memory) — shared 2.2-4.3× быстрее И корректнее (kv теряет incr/update). SoA-
таблица ~4.6× компактнее списка объектов по памяти.
Память: одна копия в Go на весь генератор, НЕ ×VU. 1M int ~24МБ, 1M объектов ~345МБ.

Порядок величин по паттернам:
| Операция | стоимость |
|---|---|
| get/at/getPath/random | ~µs, не зависит от размера данных |
| incr/add | ~µs, lock-free |
| set/setAt/setPath | ~µs |
| snapshot / get большого объекта | ∝ размеру (копия!) |
| pathSearch / pathKeys / unflatten | O(N) по дереву |
| loadCsv/loadJson (setup) | ∝ размеру файла, один раз |

**Под нагрузкой (3000 VU) — где важен contention:**
- Чтения (`get`/`random`/`at`/`getPath`/счётчики) почти не деградируют
  (RLock/atomic, параллельны). p95 стабильна во времени, очередь не растёт.
- Запись в РАЗНЫЕ ключи/списки — striping разносит по 16 полосам, конкуренции
  почти нет.
- Запись в ОДНО имя всеми VU — дорого (911µs `push`, 17ms `update`); лечится
  `*Shard`-методами: `pushShard` ~19µs (×48), `updateShard` ~4.3ms (×4).
- `update` в один ключ улучшился и без шардирования (per-key версия: 119ms→17ms).
- Корректность: 0 потерянных обновлений даже когда все VU бьют один ключ
  (проверено `go test -race` + `sum-of-counters == writes` на боевых прогонах).

---

## 9. Финальный чек-лист перед использованием

- [ ] `shared.flushAll()` первым в `setup()`
- [ ] большие данные готовятся в setup (`pushMany`/`loadJson`/`flatten`/`tablePushMany`), не в VU
- [ ] в hot-loop только `random`/`at`/`getPath`/`incr`/`tableField` — не `snapshot`/`pathSearch`
- [ ] изменить общий объект → `get`(копия)→правишь→`set`, либо `update`/`setPath` (мутация результата get в стор НЕ пишется)
- [ ] большой пул ПЛОСКИХ однотипных объектов → `table*` (память ×4.6)
- [ ] МНОГО VU пишут в ОДНО имя (очередь/аккумулятор)? → `*Shard`-методы (4.3b)
- [ ] нет спин-циклов/busy-wait вокруг shared
- [ ] для распределённого состояния (между генераторами) — Redis, не shared
- [ ] `update`.fn без побочных эффектов
```
