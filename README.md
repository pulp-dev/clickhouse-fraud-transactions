# Kafka $\rightarrow$ ClickHouse

Загрузка транзакций из CSV в Kafka, потоковое чтение топика в ClickHouse, аналитический запрос и оптимизация хранения.

Схема потока данных:

$$\texttt{train.csv} \rightarrow \texttt{producer} \rightarrow \texttt{Kafka} \rightarrow \texttt{transactions\_queue} \rightarrow \texttt{transactions\_mv} \rightarrow \texttt{transactions}$$

## Структура репозитория

| Путь | Назначение |
|---|---|
| `docker-compose.yml` | Kafka, Kafka UI, ClickHouse и producer |
| `producer/producer.py` | скрипт загрузки CSV в топик Kafka |
| `producer/Dockerfile`, `producer/requirements.txt` | образ producer |
| `requirements.txt` | зависимости для запуска producer без Docker |
| `clickhouse/init.sql` | DDL таблиц ClickHouse (выполняется при первом старте контейнера) |
| `sql/top_category_by_state.sql` | запрос: категория наибольшей транзакции по каждому штату |
| `results/top_category_by_state.csv` | результат запроса |
| `data/` | датасет (не хранится в репозитории) |

## Запуск

### 1. Требования

- Docker и Docker Compose v2 (команда `docker compose`).
- Свободные порты: `9092` (Kafka), `8080` (Kafka UI), `8123` и `9000` (ClickHouse).

### 2. Данные

Скачать `train.csv` со страницы соревнования https://www.kaggle.com/t/1918f3f6435300327d38d6c596f97394 и положить в папку `data`:

```
data/train.csv
```

### 3. Поднять контейнеры

```bash
docker compose up -d --wait
```

Команда завершится, когда Kafka и ClickHouse пройдут healthcheck. При первом старте ClickHouse выполняет `clickhouse/init.sql` и сразу подписывается на топик `transactions`.

### 4. Загрузить данные в Kafka

```bash
RPS=0 docker compose run --rm --build producer
```

RPS - контролирует скорость загрузки, когда 0, ограничений нет (так было задумано). В конце в логе должна быть строка:

```
Готово: отправлено 786431 сообщений за ... с, ошибок доставки: 0
```

## Проверка работоспособности

### Данные дошли до ClickHouse

```bash
docker exec clickhouse clickhouse-client -u user --password password \
  -q "SELECT count() FROM transactions"
```

### Запрос возвращает ожидаемый результат

```bash
diff <(docker exec -i clickhouse clickhouse-client -u user --password password \
         --format CSVWithNames < sql/top_category_by_state.sql) \
     results/top_category_by_state.csv && echo OK
```

Ожидается `OK`: результат запроса совпадает с сохраненным CSV.

Пересоздать файл результата:

```bash
docker exec -i clickhouse clickhouse-client -u user --password password \
  --format CSVWithNames < sql/top_category_by_state.sql > results/top_category_by_state.csv
```

## Оптимизация хранения

### Исходная схема

Все строки в `String`, числа в `Float64`, `ORDER BY transaction_time`.

### Оптимизированная схема

| Изменение | Колонки | Эффект |
|---|---|---|
| `LowCardinality(String)` | `us_state`, `cat_id`, `merch`, `one_city`, `street`, `name_1`, `name_2`, `jobs` | словарное кодирование: от 14 до 965 уникальных значений хранятся как числовые индексы, группировка идет по числам |
| `Decimal(9, 2)`, `Decimal(9, 4)`, `Decimal(9, 6)` вместо `Float64` | `amount`, `lat`, `lon`, `merchant_lat`, `merchant_lon` | 4 байта вместо 8, значения хранятся точно |
| `Enum8('F' = 1, 'M' = 2)` | `gender` | 1 байт на значение |
| `ORDER BY (us_state, cat_id, transaction_time)` | | колонки группировки идут первыми: сжимаются почти полностью и позволяют агрегировать в порядке ключа |
| `CODEC(Delta, ZSTD(1))` | `transaction_time` | внутри штата и категории время возрастает, хранятся разности |
| `CODEC(ZSTD(1))` | числовые колонки | более сильное сжатие, чем LZ4 по умолчанию |

### Результаты

| Метрика | До | После |
|---|---|---|
| Размер таблицы на диске | 53.9 MiB | 17.8 MiB (в 3 раза меньше) |
| Прочитано запросом | 28.9 MiB | 4.5 MiB (в 6.4 раза меньше) |
| Время запроса, медиана 10 запусков | 14 мс | 6 мс |

Размер колонок, которые читает запрос:

| Колонка | До | После |
|---|---|---|
| `us_state` | 1.21 MiB | 4.4 KiB |
| `cat_id` | 2.04 MiB | 7.9 KiB |
| `amount` | 3.31 MiB | 1.72 MiB |

На таком объеме время запроса измеряется миллисекундами и заметно колеблется, поэтому основной показатель - объем прочитанных данных.

Метрики можно получить так:

```sql
-- размер таблицы
SELECT formatReadableSize(sum(bytes_on_disk))
FROM system.parts WHERE table = 'transactions' AND active;

-- размер по колонкам
SELECT name, type, formatReadableSize(data_compressed_bytes)
FROM system.columns WHERE table = 'transactions'
ORDER BY data_compressed_bytes DESC;

-- время и объем чтения последних запросов
SYSTEM FLUSH LOGS;
SELECT query_duration_ms, read_rows, formatReadableSize(read_bytes), query
FROM system.query_log
WHERE type = 'QueryFinish' AND query ILIKE '%argMax%'
ORDER BY event_time DESC LIMIT 10;
```
