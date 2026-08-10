# Redis Full — коротка документація

## Підключення

1. Додай **redis config** та заповни host, port, TLS/ACL за потреби.
2. Обери цю config-ноду в усіх Redis-вузлах.
3. Для звичайних команд використовуй **redis cmd**: команда в полі `Command`, аргументи — у `msg.args`.

## Redis Streams: базовий надійний flow

```
[redis xadd] → Redis Stream → [redis xreadgroup] → [business logic] → [redis xack]
```

- **redis xadd** публікує `msg.payload` у Stream.
- **redis xreadgroup** читає події consumer group. Вхід відсутній, є один вихід з подією.
- Після успішної бізнес-обробки передай повідомлення в **redis xack**.
- Не вмикай `Auto ACK` для асинхронної або критичної логіки: безпечний варіант — ручний ACK після успіху.

Подія з `redis xreadgroup` має:

```js
msg.payload   // поля Redis Stream
msg.streamId  // Redis ID, потрібен для XACK
msg.stream
msg.group
msg._streamKey    // stream key for automatic redis xack routing
msg._streamGroup  // group for automatic redis xack routing
msg.consumer
```

Якщо в **redis xack** лишити поля **Stream key** та **Group** порожніми, нода автоматично використає `_streamKey` і `_streamGroup` з `redis xreadgroup`.

## Важливі поля xreadgroup

- **COUNT** — максимум записів за один виклик Redis; більший batch дає більшу пропускну здатність, але збільшує PEL.
- **BLOCK (ms)** — як довго чекати на нові записи без зайвого polling.
- **Read interval (ms)** — як часто нода викликає `XREADGROUP`, незалежно від черги: `0` = без ліміту, `1000` = максимум раз на секунду, `60000` = максимум раз на хвилину. `COUNT` лишається максимумом записів за один виклик.
- **Rate limit (msg/s)** — ліміт доставки подій у downstream за секунду; `0` = unlimited. Наприклад, `500` обмежує середню швидкість до 500 подій/с і захищає business logic.
- **Batch wait (ms)** — реальне batch-вікно: `0` відправляє кожну подію одразу; якщо більше 0, нода відправляє batch, коли набрався `COUNT` або сплив цей час. У batch-режимі `msg.payload` і `msg.streamId` — масиви, які можна напряму передати в `redis xack`.
- **Batch interval (ms)** — мінімальний інтервал між відправкою batch-ів незалежно від розміру черги. Для одного batch до 50 записів щосекунди: `COUNT = 50`, `Batch interval = 1000`. Якщо `Batch wait` лишити `0`, він автоматично дорівнюватиме цьому інтервалу.
- **Max pending** — стеля PEL; коли її досягнуто, нода не бере нові записи.
- **Auto recovery** — автоматично підхоплює завислі записи через `XAUTOCLAIM`.
- **Recovery idle** — скільки запис має бути без ACK, щоб його вважати покинутим.
- **Max deliveries** і **DLQ stream** — після надто багатьох спроб запис переходить у DLQ.

## PEL і DLQ простими словами

- **PEL (Pending Entries List)** — список подій, які Redis уже віддав конкретному consumer-у, але ще не отримав для них `XACK`. Якщо PEL росте, воркери не встигають або падають під час обробки.
- **DLQ (Dead Letter Queue)** — окремий Redis Stream для подій, які багаторазово не змогли обробитися. Вони не зникають: їх можна розібрати, виправити причину та за потреби replay-нути.
- У **redis stream metrics** задай той самий **DLQ stream**, що у `redis xreadgroup`. Результат міститиме `deadLetterLength` — поточну кількість записів у DLQ. Ненульове значення підсвічує metrics-ноду червоним.

## Кілька воркерів

Можна запускати кілька `redis xreadgroup` на одну `stream + group`: Redis розподіляє записи між ними. Поле **Consumer** краще лишати порожнім — пакет створить унікальне ім'я з hostname і Node-RED node id.

Для керованого запуску pod увімкни **Start paused (wait for resume)**. Нода створить або перевірить consumer group, але не почне `XREADGROUP` і не зарезервує записи в PEL. Після readiness-probe надішли в API `{"action":"resume"}` — для всіх local consumer-ів або з `nodeId`/`stream`/`group` для вибраних.

## HTTP-керування consumer-ами

Додай одну ноду **redis streams API**.

- За замовчуванням API вимкнене. Увімкни **Enable HTTP API**.
- У закритому контурі token не потрібен. Якщо потрібен — увімкни **Require API token** і задай token.
- Нода не має входу; її вихід можна підключити до debug.

Маршрути:

```text
GET  /redis/streams/status
POST /redis/streams/control
```

Команди для `POST /redis/streams/control`:

```json
{ "action": "pause" }
```

Зупиняє читання всіх local consumer-ів.

```json
{ "action": "resume" }
```

Поновлює читання.

```json
{ "action": "drain", "drainTimeoutMs": 30000 }
```

Зупиняє нове читання та чекає ACK вже виданих подій. Інстанс можна гасити лише коли відповідь містить `"drained": true`.

Для конкретного consumer-а спочатку візьми `nodeId` зі status, потім:

```json
{ "action": "pause", "nodeId": "35f95887218d3bc5" }
```

Також доступні фільтри `stream` і `group`.

`drained: true` означає, що PEL цього consumer-а порожній. Це не означає, що весь Stream порожній.

## Практичний production-мінімум

- Бізнес-обробка має бути ідемпотентною: можлива повторна доставка.
- Монітор `PEL`, `lag`, `deadLetterLength` та помилки через **redis stream metrics** і Node-RED logs.
- Перед масштабуванням вниз: `drain` → дочекайся `drained: true` → заверши інстанс.
- При падінні інстанса незавершені записи лишаються в PEL і підхоплюються Auto recovery.
