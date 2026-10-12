"""Эмуляция потока транзакций: читает CSV и по одной отправляет записи в топик Kafka.

Каждая строка CSV -> одно JSON-сообщение (формат JSONEachRow для ClickHouse).
Все параметры задаются переменными окружения.
"""

import json
import logging
import os
import signal
import time

import pandas as pd

from confluent_kafka import KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic


log = logging.getLogger("producer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", 3))
CSV_PATH = os.getenv("CSV_PATH", "data/train.csv")
# сообщений в секунду; 0 — без ограничения
RPS = float(os.getenv("RPS", 5000))
# отправить только первые N записей; 0 — все
LIMIT = int(os.getenv("LIMIT", 0))
SORT_BY_TIME = os.getenv("SORT_BY_TIME", "true").lower() == "true"


def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        log.info("Топик %s уже существует", topic)
        return
    fut = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])[topic]
    try:
        fut.result()
        log.info("Создан топик %s (%d партиций)", topic, partitions)
    except Exception as e:
        if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise


def load_records(path: str, sort_by_time: bool, limit: int) -> list[dict]:
    df = pd.read_csv(path)
    if sort_by_time:
        df = df.sort_values("transaction_time", kind="stable")
    if limit:
        df = df.head(limit)
    # '2019-09-14 02:46' -> '2019-09-14 02:46:00'
    df["transaction_time"] = pd.to_datetime(df["transaction_time"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    return df.to_dict(orient="records")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    ensure_topic(BOOTSTRAP_SERVERS, TOPIC, PARTITIONS)

    records = load_records(CSV_PATH, SORT_BY_TIME, LIMIT)
    total = len(records)
    log.info("Прочитано %d записей из %s, отправка в %s со скоростью %s msg/s",
             total, CSV_PATH, TOPIC, RPS or "max")

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 20,
        "compression.type": "lz4",
    })

    errors = 0

    def on_delivery(err, _msg):
        nonlocal errors
        if err is not None:
            errors += 1
            log.error("Ошибка доставки: %s", err)

    stop = False

    def on_signal(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    start = time.monotonic()
    sent = 0
    for rec in records:
        if stop:
            log.warning("Остановка по сигналу")
            break
        while True:
            try:
                producer.produce(
                    TOPIC,
                    key=rec["us_state"],
                    value=json.dumps(rec, ensure_ascii=False),
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                producer.poll(0.5)
        producer.poll(0)
        sent += 1

        if RPS > 0:
            # держим среднюю скорость: сообщение должно уйти не раньше start + sent/rps
            ahead = start + sent / RPS - time.monotonic()
            if ahead > 0:
                time.sleep(ahead)

        if sent % 10_000 == 0:
            elapsed = time.monotonic() - start
            log.info("Отправлено %d/%d (%.0f msg/s)", sent, total, sent / elapsed)

    producer.flush()
    elapsed = time.monotonic() - start
    log.info("Готово: отправлено %d сообщений за %.1f с, ошибок доставки: %d", sent, elapsed, errors)
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
