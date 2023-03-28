from sqlite3 import Connection, Cursor, Row
from typing import Union, Callable, TypeVar

import expression
import reactivex
from reactivex import Observable, operators as ops, scheduler, Observer
from reactivex.abc import ObserverBase
import sqlite3

scheduler = scheduler.ThreadPoolScheduler(1)
connection_observable: Observable[Connection] = (reactivex.from_callable(lambda: sqlite3.connect('disaster.db'))
                                                 .pipe(ops.subscribe_on(scheduler)))


def execute(cmd: str) -> Callable[[Observable[Connection]], Observable[Connection]]:
    return lambda cn_obs: cn_obs.pipe(ops.flat_map(_ex(cmd)),
                                      ops.map(lambda cu: cu.connection))


def query(query_string: str) -> Callable[[Observable[Connection]], Observable[Row]]:
    return lambda cn_obs: cn_obs.pipe(ops.flat_map(_ex(query_string)),
                                      ops.flat_map(reactivex.from_iterable))


def commit() -> Callable[[Observable[Connection]], Observable[Connection]]:
    return lambda cn_obs: cn_obs.pipe(ops.map(lambda c: c.commit() or c))


@expression.curry(1)
def _ex(query_string: str, cn: Connection) -> Observable[Cursor]:
    return reactivex.defer(lambda s: reactivex.start(lambda: cn.execute(query_string), s))


def store_trade(c: Connection, event):
    event_id = event[0]
    title = event[1]
    longitude = event[2]
    latitude = event[3]

    c.execute(f"""
        INSERT INTO disaster_event (id, title, longitude, latitude) VALUES
            ('{event_id}', '{title}', {longitude}, {latitude})""")
    c.commit()


def save(source) -> Observable[None]:
    return (connection_observable.pipe(
        execute(
            '''
            CREATE TABLE IF NOT EXISTS disaster_event(
                id text, 
                title text, 
                longitude integer, 
                latitude integer);'''),
        ops.flat_map(
            lambda connect: source.pipe(ops.observe_on(scheduler), ops.map(lambda trade: store_trade(connect, trade)))),
        ops.ignore_elements()
    ))