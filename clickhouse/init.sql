-- Потребитель топика Kafka.
-- Числа сразу разбираются в Decimal: так значения из JSON сохраняются точно
-- (Float64 превращал бы 7.94 в 7.9399999999999995).
CREATE TABLE transactions_queue
(
    transaction_time DateTime,
    merch            String,
    cat_id           String,
    amount           Decimal(9, 2),
    name_1           String,
    name_2           String,
    gender           String,
    street           String,
    one_city         String,
    us_state         String,
    post_code        UInt32,
    lat              Decimal(9, 4),
    lon              Decimal(9, 4),
    population_city  UInt32,
    jobs             String,
    merchant_lat     Decimal(9, 6),
    merchant_lon     Decimal(9, 6),
    target           UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list   = 'kafka:29092',
    kafka_topic_list    = 'transactions',
    kafka_group_name    = 'clickhouse',
    kafka_format        = 'JSONEachRow',
    kafka_num_consumers = 3;

-- Хранилище, оптимизированное под аналитические запросы:
--   LowCardinality - словарное кодирование строк с малым числом уникальных значений;
--   Decimal(9, x)  - 4 байта вместо 8 у Float64, без потери точности;
--   Enum8          - 1 байт на значение пола;
--   ORDER BY       - сначала колонки группировки (штат, категория), они сжимаются почти в ноль
--                    и позволяют агрегировать в порядке ключа;
--   CODEC          - Delta для времени (внутри штата и категории оно растет), ZSTD для чисел.
-- Партиционирование не используется: запросы не фильтруют по времени,
-- а на ~800 тыс. строк партиции только дробят данные.
CREATE TABLE transactions
(
    transaction_time DateTime                CODEC(Delta, ZSTD(1)),
    merch            LowCardinality(String),
    cat_id           LowCardinality(String),
    amount           Decimal(9, 2)           CODEC(ZSTD(1)),
    name_1           LowCardinality(String),
    name_2           LowCardinality(String),
    gender           Enum8('F' = 1, 'M' = 2),
    street           LowCardinality(String),
    one_city         LowCardinality(String),
    us_state         LowCardinality(String),
    post_code        UInt32                  CODEC(ZSTD(1)),
    lat              Decimal(9, 4)           CODEC(ZSTD(1)),
    lon              Decimal(9, 4)           CODEC(ZSTD(1)),
    population_city  UInt32                  CODEC(ZSTD(1)),
    jobs             LowCardinality(String),
    merchant_lat     Decimal(9, 6)           CODEC(ZSTD(1)),
    merchant_lon     Decimal(9, 6)           CODEC(ZSTD(1)),
    target           UInt8
)
ENGINE = MergeTree
ORDER BY (us_state, cat_id, transaction_time);

-- Перекладывает данные из очереди в хранилище (типы приводятся при вставке).
CREATE MATERIALIZED VIEW transactions_mv TO transactions AS
SELECT * FROM transactions_queue;
