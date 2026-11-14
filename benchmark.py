#!/usr/bin/env python3
"""Benchmark local de confirmación de publicación y recepción en lazo abierto."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
import csv
from dataclasses import dataclass
from pathlib import Path
from queue import SimpleQueue
import random
from statistics import median, quantiles
from threading import Lock
from time import perf_counter_ns, sleep
from uuid import uuid4

NS = 1_000_000_000
WARMUP = 32
PAYLOAD_BYTES = 1024
PROFILES = ("uniforme", "rafaga")
BROKERS = ("rabbitmq", "pubsub")
TAIL_NS = 5 * NS
FIELDS = (
    "broker", "perfil", "replica", "orden", "id", "programado_ns", "encolado_ns", "inicio_ns",
    "fin_rpc_ns", "estado", "recepcion_ns", "duplicados", "fin_carga_ns",
    "observacion_ns", "tasa_media", "segundos", "workers", "payload_bytes", "semilla",
)


@dataclass
class Message:
    due: int
    queued: int | None = None
    started: int | None = None
    ended: int | None = None
    status: str = "pendiente"
    received: int | None = None
    duplicates: int = 0


def body(message_id):
    return message_id.to_bytes(8, "big") + bytes(PAYLOAD_BYTES - 8)


@contextmanager
def rabbit(run_id, receive, cleanup):
    import pika

    params = pika.ConnectionParameters(
        host="127.0.0.1", heartbeat=20, socket_timeout=5,
        blocked_connection_timeout=5, connection_attempts=1,
    )
    queue = f"u2-{run_id}"
    with pika.BlockingConnection(params) as admin:
        admin.channel().queue_declare(queue, durable=True, arguments={"x-queue-type": "classic"})
        try:
            def consume(ready):
                with pika.BlockingConnection(params) as connection:
                    channel = connection.channel()
                    channel.basic_qos(prefetch_count=256)

                    def callback(ch, method, properties, payload):
                        receive(payload)
                        ch.basic_ack(method.delivery_tag)

                    channel.basic_consume(queue, callback, auto_ack=False)
                    ready.put(None)
                    while not cleanup():
                        connection.process_data_events(time_limit=0.05)

            @contextmanager
            def publisher():
                with pika.BlockingConnection(params) as connection:
                    channel = connection.channel()
                    channel.confirm_delivery()
                    properties = pika.BasicProperties(delivery_mode=2)

                    def publish(payload):
                        try:
                            channel.basic_publish("", queue, payload, properties, mandatory=True)
                        except (pika.exceptions.NackError, pika.exceptions.UnroutableError) as err:
                            return type(err).__name__
                        return "ok"

                    yield publish

            yield consume, publisher
        finally:
            admin.channel().queue_delete(queue)


@contextmanager
def pubsub(run_id, receive, cleanup):
    import grpc
    from google.api_core.exceptions import GoogleAPICallError
    from google.cloud.pubsub_v1 import SubscriberClient, types
    from google.pubsub_v1.services.publisher import PublisherClient
    from google.pubsub_v1.services.publisher.transports import PublisherGrpcTransport
    from google.pubsub_v1.services.subscriber.transports import SubscriberGrpcTransport

    target = "127.0.0.1:8085"
    project = "projects/benchmark-local"
    topic = f"{project}/topics/u2-{run_id}"
    subscription = f"{project}/subscriptions/u2-{run_id}"

    def transport(cls):
        return cls(channel=grpc.insecure_channel(target))

    with ExitStack() as stack:
        admin = stack.enter_context(PublisherClient(transport=transport(PublisherGrpcTransport)))
        sub = stack.enter_context(SubscriberClient(transport=transport(SubscriberGrpcTransport)))
        admin.create_topic(name=topic, retry=None, timeout=5)
        stack.callback(admin.delete_topic, topic=topic, retry=None, timeout=5)
        sub.create_subscription(name=subscription, topic=topic, retry=None, timeout=5)
        stack.callback(sub.delete_subscription, subscription=subscription, retry=None, timeout=5)

        def consume(ready):
            def callback(message):
                receive(message.data)
                message.ack()

            stream = sub.subscribe(
                subscription, callback=callback,
                flow_control=types.FlowControl(max_messages=256),
            )
            ready.put(None)
            try:
                while not cleanup():
                    if stream.done():
                        stream.result()
                        raise RuntimeError("La suscripción terminó durante el ensayo")
                    sleep(0.02)
            finally:
                stream.cancel()
                stream.result(timeout=10)

        @contextmanager
        def publisher():
            with PublisherClient(transport=transport(PublisherGrpcTransport)) as client:
                def publish(payload):
                    try:
                        response = client.publish(
                            topic=topic, messages=[types.PubsubMessage(data=payload)],
                            retry=None, timeout=5,
                        )
                    except GoogleAPICallError as err:
                        return type(err).__name__
                    if len(response.message_ids) != 1:
                        raise RuntimeError("Respuesta gRPC sin un único identificador")
                    return "ok"

                yield publish

        yield consume, publisher


def schedule(rate, seconds, profile):
    """Mantiene el volumen por segundo y concentra las llegadas en 250 ms."""
    active = NS if profile == "uniforme" else NS // 4
    return [second * NS + index * active // rate
            for second in range(seconds) for index in range(rate)]


def wait_until(instant):
    while (remaining := instant - perf_counter_ns()) > 0:
        if remaining > 350_000:
            sleep((remaining - 200_000) / NS)


def trial(broker, profile, replica, order, args):
    from threading import Event

    due = schedule(args.rate, args.seconds, profile)
    messages = [Message(instant) for instant in due]
    receive_lock = Lock()
    warmed = set()
    stop = Event()
    observation_end = [None]

    def receive(payload):
        now = perf_counter_ns()
        if len(payload) != PAYLOAD_BYTES:
            return
        idx = int.from_bytes(payload[:8], "big")
        with receive_lock:
            if idx >= len(messages):
                warmed.add(idx)
            elif observation_end[0] is not None and now <= observation_end[0]:
                message = messages[idx]
                if message.received is None:
                    message.received = now
                else:
                    message.received = min(now, message.received)
                    message.duplicates += 1

    run_id = uuid4().hex
    adapters = {"rabbitmq": rabbit, "pubsub": pubsub}
    with adapters[broker](run_id, receive, stop.is_set) as (consume, publisher):
        with ThreadPoolExecutor(max_workers=args.workers + 1) as pool:
            ready = SimpleQueue()
            consumer = pool.submit(consume, ready)
            jobs = SimpleQueue()
            go = Event()
            worker_ready = SimpleQueue()

            def work(worker):
                with publisher() as publish:
                    for offset in range(worker, WARMUP, args.workers):
                        if publish(body(len(messages) + offset)) != "ok":
                            raise RuntimeError("Falló una publicación de calentamiento")
                    worker_ready.put(worker)
                    if not go.wait(20):
                        raise RuntimeError("El generador no inició el ensayo")
                    while (idx := jobs.get()) is not None:
                        message = messages[idx]
                        payload = body(idx)
                        message.started = perf_counter_ns()
                        message.status = publish(payload)
                        message.ended = perf_counter_ns()

            workers = []
            try:
                ready.get(timeout=10)
                workers = [pool.submit(work, i) for i in range(args.workers)]
                for _ in workers:
                    worker_ready.get(timeout=10)
                warm_deadline = perf_counter_ns() + 5 * NS
                while True:
                    with receive_lock:
                        if len(warmed) == WARMUP:
                            break
                    if perf_counter_ns() >= warm_deadline:
                        raise RuntimeError("El consumidor no recibió el calentamiento")
                    sleep(0.01)
                origin = perf_counter_ns() + NS // 20
                end_load = origin + args.seconds * NS
                observation_end[0] = end_load + TAIL_NS
                go.set()
                try:
                    for idx, instant in enumerate(due):
                        wait_until(origin + instant)
                        messages[idx].queued = perf_counter_ns()
                        jobs.put(idx)
                finally:
                    for _ in workers:
                        jobs.put(None)
                wait_until(observation_end[0])
            finally:
                stop.set()
                if not go.is_set():
                    for _ in workers:
                        jobs.put(None)
                    go.set()
            for future in workers:
                future.result()
            consumer.result()

    return [{
        "broker": broker, "perfil": profile, "replica": replica, "orden": order,
        "id": idx, "programado_ns": m.due, "encolado_ns": m.queued - origin,
        "inicio_ns": m.started - origin,
        "fin_rpc_ns": m.ended - origin, "estado": m.status,
        "recepcion_ns": "" if m.received is None else m.received - origin,
        "duplicados": m.duplicates, "fin_carga_ns": args.seconds * NS,
        "observacion_ns": args.seconds * NS + TAIL_NS, "tasa_media": args.rate,
        "segundos": args.seconds, "workers": args.workers, "payload_bytes": PAYLOAD_BYTES,
        "semilla": args.seed,
    } for idx, m in enumerate(messages)]


def read_results(path):
    grouped = {}
    with Path(path).open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            grouped.setdefault((row["broker"], row["perfil"], int(row["replica"])), []).append(row)
    return grouped


def summarize(grouped, grace=0.5):
    """Una fila por ensayo. Los percentiles se calculan sobre confirmaciones positivas."""
    records = []
    for (broker, profile, replica), rows in sorted(grouped.items()):
        end = int(rows[0]["fin_carga_ns"])
        cutoff = end + round(grace * NS)
        if cutoff > int(rows[0]["observacion_ns"]):
            raise ValueError("El plazo excede la ventana registrada")
        ok = [r for r in rows if r["estado"] == "ok"]
        seen = [r for r in rows if r["recepcion_ns"] and int(r["recepcion_ns"]) <= cutoff]
        loss = len(rows) - len(seen)

        def times(items, after, before):
            return [(int(r[after]) - int(r[before])) / 1e6 for r in items]

        total = times(ok, "fin_rpc_ns", "programado_ns")
        rpc = times(ok, "fin_rpc_ns", "inicio_ns")
        wait = times(rows, "inicio_ns", "programado_ns")
        records.append(dict(
            broker=broker, perfil=profile, replica=replica, n=len(rows), confirmadas=len(ok),
            p50=median(total) if total else float("nan"),
            p95=quantiles(total, n=100, method="inclusive")[94] if len(total) > 1 else
                (total[0] if total else float("nan")),
            p99=quantiles(total, n=100, method="inclusive")[98] if len(total) > 1 else
                (total[0] if total else float("nan")),
            rpc50=median(rpc) if rpc else float("nan"),
            espera50=median(wait), ausentes=loss, perdida=100 * loss / len(rows),
            confirmadas_ausentes=sum(r["estado"] == "ok" and
                (not r["recepcion_ns"] or int(r["recepcion_ns"]) > cutoff) for r in rows),
            duplicados=sum(int(r["duplicados"]) for r in rows),
        ))
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=int, default=1000, help="Mensajes/segundo en promedio")
    parser.add_argument("--seconds", type=int, default=2)
    parser.add_argument("--reps", type=int, default=8)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--seed", type=int, default=260613)
    parser.add_argument("--output", type=Path, default=Path("results.csv"))
    args = parser.parse_args()
    if min(args.rate, args.seconds, args.reps, args.workers) < 1 or args.workers > WARMUP:
        parser.error("Los tamaños deben ser positivos y workers debe ser ≤ 32")
    rng = random.Random(args.seed)
    cases = [(broker, profile) for broker in BROKERS for profile in PROFILES]
    rows = []
    for replica in range(1, args.reps + 1):
        block = cases.copy()
        rng.shuffle(block)
        for order, (broker, profile) in enumerate(block, start=1):
            print(f"réplica {replica:02d} | {order} | {broker:8s} | {profile}", flush=True)
            rows.extend(trial(broker, profile, replica, order, args))

    temporary = args.output.with_name(args.output.name + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(args.output)
    print(f"{len(rows):,} mensajes registrados en {args.output}")


if __name__ == "__main__":
    main()
