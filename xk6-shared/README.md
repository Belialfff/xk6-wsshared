# xk6-shared — общие переменные между VU внутри одного `k6 run`

In-process разделяемое изменяемое состояние для k6. Все VU одного генератора
видят один и тот же стор — как обычные глобальные переменные, но работающие
между потоками (VU). In-process аналог Redis: без сети и сериализации на
горячем пути, доступ за наносекунды.

- **Область видимости:** один процесс `k6 run` (один генератор). НЕ шарится
  между распределёнными генераторами — для этого используйте Redis.
- **Почему не JS-глобалы:** каждый VU в k6 — отдельный JS-isolate, их глобальные
  переменные не видны друг другу. Расширение живёт в одном Go-процессе, поэтому
  его состояние общее для всех VU.

---

## ⚠️ Про безопасность чтения (прочти первым)

**`get`/`at`/`random`/`getPath`/`tableRow`/`tableRandom`/`tableField`/`snapshot`/
`unflatten` возвращают КОПИЮ объекта — мутировать результат безопасно ВСЕГДА,
стор не тронется, краш невозможен.** (Композиты копируются на выходе, скаляры —
бесплатно.)

```js
// ✅ безопасно — это копия, стор не меняется:
const cfg = shared.get('cfg');
cfg.limit = 999;                 // меняет ЛОКАЛЬНУЮ копию, не стор
// чтобы ЗАПИСАТЬ в стор — только через методы:
shared.set('cfg', cfg);          // замена целиком под локом
shared.setPath('cfg', 'limit', 999);
shared.update('cfg', c => { c.limit++; return c; });   // атомарно
```

> ⚠️ Не рассчитывай, что мутация результата `get` «запишется» — это КОПИЯ.
> Чтобы изменение попало в общий стор, вызови `set`/`setPath`/`update`.

---

## Установка / сборка

Требуется: Go, [xk6](https://github.com/grafana/xk6).

```bash
cd sb2.k6.tests.load/k6-extensions/xk6-shared
xk6 build v1.1.0 --with github.com/altenar/xk6-shared="$(pwd -W)" --output ./k6-shared.exe
```

> Windows: путь ОБЯЗАТЕЛЬНО нативный (`pwd -W` → `C:/...`), не `/c/...`, иначе
> `replacement directory does not exist`. Если упало на `compiling` — повторите
> (разовая гонка очистки temp на Windows).

Запуск теста собранным бинарём:
```bash
./k6-shared.exe run your-test.js
```

TS/JS-подсказки — файл [`shared.d.ts`](./shared.d.ts).

---

## Импорт

```js
import shared from 'k6/x/shared';
```

## Поддерживаемые типы значений

`string`, `number` (int/float, до 2^53), `boolean`, `null`, массивы и объекты
(включая вложенные, массивы объектов).

Нюансы (штатные, как в JSON):
- **Массивы** — порядок элементов сохраняется.
- **Объекты** — порядок КЛЮЧЕЙ не гарантируется. Обращайтесь по имени поля.
- `undefined` сохраняется как `null`.
- Нельзя хранить функции, `Date`, `Map`/`Set`, циклические ссылки.
- `set(k, obj)` хранит по КОПИИ: изменение исходного JS-объекта после `set`
  не меняет стор. `get` тоже возвращает КОПИЮ — мутировать безопасно (см. выше).

---

# API

Быстрая карта: **`get*` читает • `set*` пишет • `update*` атомарно меняет**

| Данные | Читать | Писать | Атомарно менять |
|---|---|---|---|
| переменная | `get` | `set` | `update` |
| элемент списка | `at` / `random` | `setAt` | — |
| поле дерева | `getPath` | `setPath` | `updatePath` |
| счётчик | `count` | `incr` / `add` | (сам атомарен) |

---

## 📦 Справочник: что куда заливать · как вызвать · что в ответе

Ниже — по каждой структуре: **какие данные кладём (как выглядят), каким методом,
и ЧТО возвращается**. Копируй блоки как есть.

### A. Скаляр / объект — одна именованная переменная
Данные: строка, число, boolean, объект, массив — что угодно JSON-сериализуемое.
```js
// ЗАЛИВАЕМ
shared.set('integration', 'brand-a');                 // строка
shared.set('limit', 500);                             // число
shared.set('cfg', { code: 'EUR', limit: 500, live: true });   // объект
shared.set('ids', [101, 202, 303]);                   // массив

// ЧИТАЕМ (возвращается КОПИЯ — мутировать безопасно)
const s   = shared.get('integration');   // 'brand-a'      (string)  | null если нет
const n   = shared.get('limit');         // 500            (number)
const cfg = shared.get('cfg');           // { code:'EUR', limit:500, live:true }  (object)
cfg.limit = 999;                         // ✅ меняет КОПИЮ, стор не тронут
shared.has('cfg');                       // true | false
shared.delete('limit');                  // (void)
```

### B. Атомарный счётчик — общее число между всеми VU
Данные: целое число (внутри int64). Ключ создаётся при первом `incr`/`add`.
```js
// МЕНЯЕМ (возврат — НОВОЕ значение)
const v1 = shared.incr('placedBets');    // было 0 → вернёт 1  (number)
const v2 = shared.add('totalStake', 50); // +50 → вернёт сумму (number)
// ЧИТАЕМ
const c  = shared.count('placedBets');   // текущее, 0 если не трогали (number)
shared.resetCounter('placedBets');       // сброс в 0 (void)
```

### C. Список / пул — растущий массив значений (в т.ч. объектов)
Данные: массив любых значений. Наполняем в `setup`, читаем в VU.
```js
// ЗАЛИВАЕМ (в setup)
shared.pushMany('eventPool', ['13381688:brand-b', '13381700:brand-a']);  // строки
shared.pushMany('records', [{ id: 1, odds: 1.5 }, { id: 2, odds: 2.0 }]); // объекты
shared.push('eventPool', '13381999:x');   // добавить один → вернёт новую длину (number)
shared.pushUnique('seen', 12345);         // добавить если нет → true/false (bool)

// ЧИТАЕМ в VU (возврат — КОПИЯ элемента)
const e   = shared.random('eventPool');   // случайный элемент       | null если пусто
const one = shared.at('records', 0);      // { id:1, odds:1.5 }      | null вне диапазона
const n   = shared.size('eventPool');     // длина (number)
const job = shared.shift('queue');        // снять первый (FIFO)     | null если пусто
const top = shared.pop('stack');          // снять последний (LIFO)  | null если пусто
const all = shared.snapshot('eventPool'); // КОПИЯ всего списка (массив) — не в hot-loop!
```

### D. Дерево (flatten) — вложенный объект, доступ по пути
Данные: один вложенный объект. Раскладываем один раз, читаем/пишем точечно.
```js
// ЗАЛИВАЕМ (в setup)
shared.flatten('cfg', { wallet: { code: 'EUR', limit: 500 }, events: [{ id: 1 }] });

// ЧИТАЕМ / ПИШЕМ ЛИСТ
const code = shared.getPath('cfg', 'wallet.code');   // 'EUR'  (копия листа) | null
const eid  = shared.getPath('cfg', 'events.0.id');   // 1
shared.setPath('cfg', 'wallet.limit', 999);          // записать лист → true
shared.updatePath('cfg', 'wallet.limit', v => v+1);  // атомарно → новое значение
shared.hasPath('cfg', 'wallet.code');                // true | false
const whole = shared.unflatten('cfg');               // собрать объект обратно (копия)
shared.treeClear('cfg');                             // удалить всё дерево (void)
```

### E. Колоночная таблица (SoA) — большой пул ОДНОТИПНЫХ плоских объектов
Данные: массив плоских объектов ОДНОЙ формы (event-пул, строки юзеров). ×4.6 память.
```js
// ЗАЛИВАЕМ (в setup) — схема выводится сама из первого объекта
shared.tablePushMany('events', [
  { id: 13381688, status: 'open',   odds: 1.5, integration: 'brand-b' },
  { id: 13381700, status: 'closed', odds: 2.0, integration: 'brand-a' },
]);
shared.tablePush('events', { id: 13381999, status: 'open', odds: 3.0, integration: 'x' });

// ЧИТАЕМ в VU (возврат — КОПИЯ)
const row  = shared.tableRandom('events');           // { id:.., status:.., odds:.., integration:.. } | null
const r3   = shared.tableRow('events', 3);           // строка по индексу (object) | null
const odds = shared.tableField('events', 3, 'odds'); // ОДНО поле, дёшево → 1.5 | null если нет
const cnt  = shared.tableSize('events');             // число строк (number)
shared.tableClear('events');                         // очистить (void)
```

### F. Авто-шардированные — когда ТЫСЯЧИ VU пишут в ОДНО имя
Данные: как у списков/аккумулятора, но запись раскидана по 16 под-шардам (без locks-борьбы).
```js
// ОЧЕРЕДЬ под массовой записью (producer → consumer)
shared.pushShard('cashout', { userToken: 't', betId: 42 });  // как push, но контеншен-фри
const ctx = shared.shiftAny('cashout');              // снять из любого шарда | null
const sz  = shared.sizeShard('cashout');             // суммарный размер (number)

// АККУМУЛЯТОР (каждый VU в свой шард → лок-фри), слить при чтении
shared.updateShard('stats', s => { s = s || {n:0}; s.n++; return s; });
const total = shared.readShard('stats',              // → { n: <сумма по всем шардам> }
  (acc, part) => ({ n: acc.n + (part ? part.n : 0) }), () => ({ n: 0 }));
```

### G. Go-загрузчики — распарсить файл в Go (не в JS)
Данные: текст файла через `open()`. Парсинг один раз, без нагрузки на JS-isolate.
```js
shared.loadCsv('users', open('users.csv'));      // CSV → пул объектов по заголовку → размер (number)
shared.loadJson('cfg', open('config.json'));     // JSON → дерево (как flatten) → число листьев
shared.loadJsonPool('ev', open('events.json'));  // JSON-массив → пул записей → размер
```

> **Все читающие методы (`get`/`at`/`random`/`getPath`/`tableRow`/`tableRandom`/
> `tableField`/`snapshot`/`unflatten`) возвращают КОПИЮ** — мутировать результат
> безопасно всегда, стор не тронется, краш невозможен. Записать изменение в стор
> — только через `set`/`setPath`/`update`. Скаляры копируются бесплатно.

---

## 1. Скалярные переменные

```js
shared.set('integration', 'brand-a');   // сохранить (перезапись)
shared.get('integration');              // прочитать; null если нет
shared.has('cfg');                      // boolean
shared.delete('enabled');               // удалить
shared.setIfAbsent('once', true);       // записать только если ключа нет → bool ("первый VU выигрывает")
shared.compareAndSwap('leader', null, __VU);  // set next, только если текущее == expected → bool
```

### `update(key, fn)` → новое значение
Атомарный read-modify-write. `fn` получает текущее значение и **возвращает**
новое. Безопасно даже когда 30k VU меняют ОДИН ключ (нет потерянных обновлений).
```js
shared.set('n', 0);
shared.update('n', v => v + 1);                 // атомарный инкремент
shared.update('cfg', c => { c.limit += 1; return c; });   // изменить поле объекта безопасно
```
> `fn` может вызваться повторно при конкуренции (оптимистичный retry) — он
> должен только вычислять новое значение, без побочных эффектов.

## 2. Атомарные счётчики (lock-free)

```js
shared.incr('placedBets');     // +1 → новое значение
shared.add('totalStake', 50);  // +delta → новое значение
shared.count('placedBets');    // текущее (0 если не трогали)
shared.resetCounter('placedBets');
```

## 3. Пулы / списки (растут, меняются в любой момент)

> Горячий путь: `random`/`at`/`size` копируют ОДИН элемент, а не весь пул.

```js
shared.push('pool', 12345);            // добавить в конец → новая длина
shared.pushUnique('pool', 12345);      // добавить если нет (по строковой форме) → bool, O(1)
shared.pushMany('pool', [1, 2, 3, 4]); // добавить пачкой за одну блокировку → длина
shared.size('pool');                   // длина
shared.at('pool', 0);                  // элемент по индексу (отрицательный — с конца), null вне диапазона
shared.setAt('pool', 0, 999);          // ЗАМЕНИТЬ элемент по индексу (потокобезопасно) → bool
shared.random('pool');                 // случайный элемент (горячий путь)
shared.randomN('pool', 3);             // до N различных случайных
shared.pop('pool');                    // снять последний (LIFO), null если пусто
shared.shift('pool');                  // снять первый (FIFO/очередь), null если пусто
shared.listHas('pool', 12345);         // есть ли в индексе уникальности (для pushUnique-списков)
shared.snapshot('pool');               // КОПИЯ всего списка — не на горячем пути!
shared.clearList('pool');              // очистить список и индекс
```

## 3b. Авто-шардированные структуры — когда МНОГО VU пишут в ОДНО имя

Та же семантика, что у `push`/`pushUnique`/`shift`/`random`/`update`, но запись
раскидывается по 16 внутренним под-ключам (по номеру VU). Тысячи писателей НЕ
дерутся за один лок. В тесте ничего не добавляешь — методы шардируют сами.
Читатели (`shiftAny`/`randomShard`/`sizeShard`/`snapshotShard`) собирают всё
обратно по всем шардам.

```js
// ОЧЕРЕДЬ / СПИСОК под массовой записью:
shared.pushShard('queue', job);        // как push, но контеншен-фри между VU → длина шарда
shared.pushUniqueShard('seen', x);     // как pushUnique (уникальность В ПРЕДЕЛАХ шарда)
shared.sizeShard('queue');             // СУММАРНАЯ длина по всем шардам
shared.shiftAny('queue');              // снять из любого непустого (FIFO в шарде), null если пусто
shared.popAny('queue');                // LIFO из любого непустого
shared.randomShard('queue');           // равномерный случайный по всему логическому списку
shared.snapshotShard('queue');         // КОПИЯ всего (все шарды) — setup/debug
shared.clearShard('queue');            // очистить все шарды

// АККУМУЛЯТОР под массовой записью (каждый VU пишет в свой шард → лок-фри):
shared.updateShard('stats', s => { s = s || {n:0}; s.n++; return s; });
// слить 16 шардов в общий результат:
const total = shared.readShard('stats',
    (acc, part) => { acc.n += (part ? part.n : 0); return acc; },  // merge
    () => ({ n: 0 })                                               // seed
);
```

> Плата за отсутствие лока: `shiftAny` — НЕ строгий глобальный FIFO (FIFO внутри
> шарда, между шардами round-robin). `pushUniqueShard` дедуплицирует в пределах
> шарда, не глобально. Для строгого FIFO / глобальной уникальности — обычные
> `shift`/`pushUnique` (ценой контеншена). Замер: `pushShard` ~19µs против
> `push` в 1 список 911µs (×48); `updateShard` ~4.3ms против `update` 17ms.

## 4. Дерево (flatten) — точечный доступ к вложенному объекту

Разложить объект один раз в пути `a.b.c`, потом читать/менять точечно —
без копирования всего объекта.

```js
shared.flatten('cfg', { wallet: { code: 'EUR', limit: 500 }, events: [{ id: 1 }] });
// листья: wallet.code, wallet.limit, events.0.id

shared.getPath('cfg', 'wallet.code');       // 'EUR'  (O(1))
shared.getPath('cfg', 'events.0.id');       // 1
shared.setPath('cfg', 'wallet.limit', 999); // ЗАПИСЬ листа (потокобезопасно)
shared.updatePath('cfg', 'wallet.limit', v => v + 1);  // атомарный read-modify-write листа
shared.hasPath('cfg', 'wallet.code');       // boolean
shared.deletePath('cfg', 'wallet.code');
shared.treeSize('cfg');                     // число листьев

shared.pathKeys('cfg', '');                 // все пути; с префиксом: pathKeys('cfg','wallet.')
shared.pathSearch('cfg', 'eventId');        // пути, содержащие подстроку — O(N), ТОЛЬКО в setup!
shared.unflatten('cfg');                    // собрать объект обратно (массивы восстанавливаются)
shared.treeClear('cfg');                    // удалить всё дерево целиком
```

> **`pathSearch` — линейный скан.** Не вызывать в hot-loop на 30k VU. Ищи один
> раз в `setup`, результат клади в пул, в рантайме бери `random`/`at` (пример ниже).

## 4b. Колоночные таблицы (SoA) — экономия памяти на пулах однотипных объектов

Для БОЛЬШОГО пула **плоских однотипных** записей (event-пул, строки пользователей):
хранит колоночно (structure-of-arrays) — один типизированный слайс на поле вместо
N отдельных map. **~4.6× меньше памяти** (замер: 500k строк 179МБ→39МБ). Пушишь
объекты как в список — **схема выводится сама** из первого объекта и растёт, если
у позднего объекта появилось новое поле (старым строкам — дефолт).

```js
shared.tablePushMany('events', arr);   // схема сама; arr — массив плоских объектов
shared.tablePush('events', obj);       // по одному
shared.tableRandom('events');          // случайная строка-объект (КОПИЯ)
shared.tableRow('events', 3);          // строка по индексу
shared.tableField('events', 3, 'odds');// ОДНО поле без сборки всей строки (дёшево)
shared.tableSize('events');            // число строк
shared.tableClear('events');           // очистить таблицу
```

> Только для **плоских однотипных** данных. Вложенные/разношёрстные поля падают в
> boxed-колонку (работает, но без экономии). Отсутствующее поле читается как
> дефолт (`0`/`""`/`false`), несуществующее в `tableField` → `null` (не ошибка).
> Если поле в разных объектах разного типа — колонка промотируется в boxed (ничего
> не теряется). Для вложенных объектов — обычные `set`/`push`/`flatten`.

## 5. Go-загрузчики — парсинг файла в Go, не в JS

Файл не проходит через JS-isolate: парсится один раз в Go, живёт одной копией.

```js
shared.loadCsv('rows', open('big.csv'));      // → пул объектов-строк по заголовку → размер
shared.loadJson('cfg', open('config.json'));  // → дерево (как flatten) → число листьев
shared.loadJsonPool('ev', open('events.json'));  // JSON-МАССИВ → пул записей → размер
```

## 6. Интроспекция / жизненный цикл

```js
shared.keys();      // имена всех скалярных переменных
shared.lists();     // имена всех списков
shared.flushAll();  // очистить весь стор — вызывать в setup()
```

---

# Полные примеры

## Собрать пул на старте, читать всеми VU

```js
import shared from 'k6/x/shared';

export function setup() {
    shared.flushAll();
    if (shared.setIfAbsent('pool:built', true)) {   // строит ровно один VU
        shared.pushMany('eventPool', [101, 202, 303, 404, 505]);
    }
}
export default function () {
    const eventId = shared.random('eventPool');     // горячий путь
    // ... placeBet(eventId) ...
}
```

## Очередь заданий между VU (producer / consumer)

```js
export default function () {
    if (exec.vu.idInInstance <= 2) {
        shared.push('jobs', { id: shared.incr('jobId') });   // продюсеры
    } else {
        const job = shared.shift('jobs');                    // консьюмеры (FIFO)
        if (job !== null) { /* обработать */ }
    }
}
```

## Атомарный общий счётчик-объект

```js
export function setup() { shared.flushAll(); shared.set('stats', { placed: 0, failed: 0 }); }
export default function () {
    // безопасно даже если все VU бьют один объект:
    shared.update('stats', s => { s.placed += 1; return s; });
}
```

## Большой файл всем потокам (правильно — через пул)

```js
export function setup() {
    shared.flushAll();
    shared.loadCsv('users', open('users_200mb.csv'));  // парсинг в Go, одна копия
}
export default function () {
    const user = shared.random('users');   // ~2µs, размер файла не важен
}
```

## Поиск по дереву — один раз в setup, кэш в пул

```js
export function setup() {
    shared.flushAll();
    shared.loadJson('big', open('big.json'));
    const idPaths = shared.pathSearch('big', 'eventId');  // дорого — но ОДИН раз
    shared.pushMany('idPathCache', idPaths);              // кэш
}
export default function () {
    const p = shared.random('idPathCache');   // дёшево
    const v = shared.getPath('big', p);       // O(1)
}
```

---

# Как это работает внутри (internals)

**Процесс.** Один `k6 run` — один ОС-процесс, одна Go-runtime. Каждый VU — это
goroutine + свой sobek (goja) isolate со своей JS-кучей. JS-кучи изолированы:
глобалка одного VU недостижима из другого. Наш `globalStore` — package-level
Go-структура в общем Go-heap процесса → её видят все goroutine. Отсюда шаринг.

**Вызов.** `shared.get(...)` из JS вызывает наш Go-метод в ТОЙ ЖЕ goroutine
(не поток, не сеть, не IPC) — обычный вызов функции через мост sobek↔Go.

**Граница Go→JS (ключевое).**
- `string`/`number`/`bool`/`null` — отдаются по значению.
- `map`/`slice` — отдаются **живой обёрткой** над той же Go-структурой, sobek
  НЕ копирует. Поэтому `get` большого объекта дёшев (данные не копируются,
  одна копия в Go), но `.field = x` пишет в общую map напрямую.

**Большие объекты.**
- `get('big')` → дёшево (обёртка, 30k VU = 30k обёрток + 1 копия данных).
- Полный обход/`JSON.stringify` в JS → дорого (переход границы на каждое поле).
- `set(hugeArray)` из JS → дорого (поэлементная конверсия JS→Go).
- **THIN:** `pushMany` один раз, потом `random`/`at` → через границу едет один
  элемент, ~2µs независимо от размера пула. Это оптимум для больших данных.

**Гонки.** Go-map не потокобезопасна (две записи разом → краш рантайма).
- **Lock striping:** каждый домен (переменные / списки / деревья) разбит на
  16 шардов по хешу ключа. Писатели в РАЗНЫЕ ключи/списки попадают в разные
  шарды → разные мьютексы → не мешают друг другу (как `ConcurrentHashMap`).
- Счётчики — `atomic.AddInt64`, без мьютекса вообще.
- Мутация ЖИВОЙ обёртки (`get(k).field = x`) обходит мьютекс → гонка/краш. Это
  и есть причина правила безопасности выше.
- `update*` — оптимистичный CAS с **версией на КЛЮЧ** (не на весь стор): колбэк
  работает с изолированной копией (`deepCopy` только для объектов/массивов;
  скаляры иммутабельны — без копии), ретрай только если ИМЕННО этот ключ
  изменился под нами. Колбэк выполняется вне мьютекса.

**⚡ Горячая запись в ОДНУ точку всеми VU — используй `*Shard`-методы.** Striping
спасает когда ключи РАЗНЫЕ. Если 3000 VU пишут в один список/ключ — все в один
шард (потолок физический). Готовые методы шардируют ВНУТРИ сами, в тесте ничего
добавлять не надо (см. раздел «Авто-шардированные структуры» ниже):
```js
shared.pushShard('queue', job);      // вместо push('queue')   → ×48 throughput
shared.updateShard('acc', fn);       // вместо update('acc')   → лок-фри на запись
// читатели собирают всё обратно:
const job = shared.shiftAny('queue');            // из любого непустого шарда
const total = shared.readShard('acc', merge, seed);  // слить 16 шардов
```
Замер (3000 VU): `push` в 1 список 911µs → `pushShard` **~19µs**; `update`
объекта в 1 ключ 17ms → `updateShard` **~4.3ms** (а per-key версия сама по себе
уже срезала обычный `update` с 119ms до 17ms без изменений в тесте).

---

# Производительность (замеры, 16 ядер)

Одна операция без конкуренции (Go-бенч):

| Операция | ns/op | allocs | | Память | на элемент |
|---|---|---|---|---|---|
| get | 14 | 0 | | 1M int в пуле | ~26 Б |
| at | 22 | 0 | | 1M объектов | ~362 Б |
| getPath | 30 | 0 | | 100k pushUnique | +индекс ~2× |
| set | 31 | 0 | | | |
| random | 37 | 0 | | (одна копия в Go, | |
| incr | 66 | 2 | | не ×VU) | |

Под нагрузкой (3000 VU) чтения (`get`/`random`/`at`/`getPath`/счётчики) почти
не деградируют (RLock/atomic). Дорого — только массовая запись в ОДНУ точку;
лечится шардированием ключа (см. выше).

---

# Ограничения

- Область — один процесс `k6 run`. Между машинами не шарится (для этого Redis).
- Безопасность мутаций держится на ДИСЦИПЛИНЕ (`get` только читать): API не
  может физически запретить мутацию живой ссылки. Формально ловится
  `go test -race` — прогоняется чисто (в т.ч. на Windows, mingw-gcc).
- Порядок ключей объектов не гарантируется.
- Живые коннекты (БД/Redis/Kafka) через `set` не хранить — см.
  `../../../shared-check/CONNECTIONS-PLAN.md` (отложенный план).
- MVP+: не встроено в `runload.sh`/CI.
