import os
import sqlite3
from calendar import timegm
from datetime import datetime, timedelta
from time import gmtime

import constants
from constants import JOB_TYPE_YEAR, JOB_TYPE_DAY, JOB_TYPE_HOUR, JOB_TYPE_MONTH
from rollup import rollup_sensor
from time_series import Timeseries
from util import epoch_to_date, int_if_possible

TS = constants.TS_TS
VALUE = constants.TS_VALUE
STATUS = constants.TS_STATUS


class Datastore:
    @staticmethod
    def fetchall(query, conn):
        cur = conn.cursor()
        cur.execute(query)
        return cur.fetchall()

    @staticmethod
    def _rebuild(conn):
        create_stm = [
            "DROP TABLE IF EXISTS rollup",
            "DROP TABLE IF EXISTS series",
            "DROP TABLE IF EXISTS sensors",
            "PRAGMA foreign_keys=OFF;",
            '''CREATE TABLE sensors (
              id           integer PRIMARY KEY AUTOINCREMENT,
              name         text NOT NULL,
              tags         json NOT NULL DEFAULT '{}',
              /* Keys */
              CONSTRAINT sensors_idx_name
                UNIQUE (name)
            );''',
            '''CREATE TABLE series (
              id         integer PRIMARY KEY AUTOINCREMENT,
              sensor_id integer NOT NULL REFERENCES sensors(id) ON DELETE CASCADE,
              ts         integer NOT NULL,
              value      real NOT NULL,
              status     integer NOT NULL DEFAULT 0,
              /* Keys */
              UNIQUE (sensor_id, ts) ON CONFLICT REPLACE
            );''',
            '''CREATE TABLE rollup (
              id         integer PRIMARY KEY AUTOINCREMENT,
              sensor_id  integer NOT NULL REFERENCES sensors(id) ON DELETE CASCADE,
              ts         integer,
              type       integer,
              vmin       real,
              vmax       real,
              vavg       real,
              vsum       real,
              vcount     integer,
              /* Keys */
              CONSTRAINT rollup_unique_idx
                UNIQUE (sensor_id, type, ts)
            );''',
            '''CREATE INDEX idx_data_store_id_sec
              ON series
              (sensor_id, ts);''',
            '''CREATE INDEX idx_data_store_sec
              ON series
              (ts);'''
        ]
        for cmd in create_stm:
            print(cmd)
            conn.execute(cmd)

    def __init__(self, database=None, user_name=None, password=None, create=False):
        if database is None:
            database = "default.db3"
        if not database.endswith(".db3"):
            database += ".db3"
        new_file = False
        if not os.path.exists(database):
            new_file = True
            if not create:
                raise ValueError("Database not found")
        self.database = database
        self.user_name = user_name
        self.password = password
        self.conn = sqlite3.connect(database)
        self.conn.execute("PRAGMA foreign_keys = ON;")
        if new_file and create:
            self._rebuild(self.conn)
        self.is_opened = True

    def close(self):
        if self.is_opened:
            self.conn.close()
            self.is_opened = False

    def rebuild(self):
        self._rebuild(self.conn)

    @staticmethod
    def get_sensor_id(conn, sensor_name):
        cur = conn.cursor()
        q = "SELECT id FROM sensors WHERE name='{}'".format(sensor_name)
        cur.execute(q)
        rows = cur.fetchone()
        if rows is None:
            return rows
        return rows[0]

    @staticmethod
    def get_ts_hour(time_stamp):
        tm = gmtime(time_stamp)
        dt = datetime(tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, 0, 0)
        tt = dt.timetuple()
        time_stamp = timegm(tt)
        return time_stamp

    def data_create(self, time_series):
        jobs = {}
        id = self.get_sensor_id(self.conn, time_series.name)
        cur = self.conn.cursor()
        if id is None:
            q = "INSERT INTO sensors (name) VALUES ('{}')".format(time_series.name)
            cur.execute(q)
            self.conn.commit()
            id = cur.lastrowid
        q = "INSERT INTO series (sensor_id, ts, value, status) VALUES "
        for dp_count, dp in enumerate(time_series.data_points):
            if dp_count > 0:
                q += ",\n"
            row = f"({id}, {dp[TS]}, {dp[VALUE]}, {dp[STATUS]})"
            q += row
            hour = self.get_ts_hour(dp[TS])
            jobs[hour] = 1
        cur.execute(q)
        self.conn.commit()
        rollup_sensor(self.conn, id, jobs)

    def data_read_interval(self, sensor_name, start_date, end_date):
        """
        Read series from the interval table
        :param sensor_name: The sensor name
        :param start_date: The start date
        :param end_date: The end date
        :return: Time series
        """
        t = Timeseries(sensor_name)
        sd = t.to_epoch(start_date)
        ed = t.to_epoch(end_date)
        if sd is None or ed is None:
            raise ValueError("Invalid date")
        q = '''
            SELECT h.ts, h.value, h.status FROM series h
            JOIN sensors s ON (h.sensor_id = s.id)
            AND s.name = '{}'
            AND ts >= {} 
            AND ts <= {}
        '''
        query = q.format(sensor_name, sd, ed)
        rows = self.fetchall(query, self.conn)
        for row in rows:
            t.add_data_point(row[0], row[1], row[2])
        return t

    def data_read_rollup(self, sensor_name, start_date, end_date, group_by, function):
        t = Timeseries(sensor_name)
        sd = t.to_epoch(start_date)
        ed = t.to_epoch(end_date)
        if sd is None or ed is None:
            raise ValueError("Invalid date")
        groups = [
            "hour", "day", "month", "year"
        ]
        if group_by not in groups:
            raise ValueError("Invalid group")
        functions = [
            "max", "min", "count", "sum", "avg", "first", "last"
        ]
        if function not in functions:
            raise ValueError("Invalid function")
        if group_by == "day":
            rollup_type = JOB_TYPE_DAY
        elif group_by == "hour":
            rollup_type = JOB_TYPE_HOUR
        elif group_by == "month":
            rollup_type = JOB_TYPE_MONTH
        else:
            rollup_type = JOB_TYPE_YEAR
        if function in ["first", "last"]:
            raise ValueError("Function not implemented")
        q = '''
            SELECT h.ts, h.v{}, 0 FROM rollup h
            JOIN sensors s ON (h.sensor_id = s.id)
            AND s.name = '{}'
            AND type = {}
            AND ts >= {} 
            AND ts <= {}
        '''
        query = q.format(function, sensor_name, rollup_type, sd, ed)
        rows = self.fetchall(query, self.conn)
        for row in rows:
            t.add_data_point(row[0], row[1], row[2])
        return t

    @staticmethod
    def translate_rollup_type(rollup_type):
        if rollup_type == JOB_TYPE_DAY:
            return "daily"
        if rollup_type == JOB_TYPE_MONTH:
            return "monthly"
        if rollup_type == JOB_TYPE_YEAR:
            return "yearly"
        if rollup_type == JOB_TYPE_HOUR:
            return "hourly"
        return "?"

    def dump_interval(self, start_index, limit):
        q = '''
            SELECT id, sensor_id, ts, value, status
            FROM series
            WHERE id >= {}
            LIMIT {}
        '''
        query = q.format(start_index, limit)
        rows = self.fetchall(query, self.conn)
        d = []
        for row in rows:
            dt = epoch_to_date(row[2]).strftime("%Y-%m-%dT%H:%M:%S")
            item = {
                "id": row[0],
                "sensor_id": row[1],
                "ts": dt,
                "value": int_if_possible(row[3]),
                "status": row[4]
            }
            d.append(item)
        return d

    def dump_rollup(self, start_index, limit):
        q = '''
            SELECT id, sensor_id, type, ts, vmax, vmin, vsum, vcount, vavg
            FROM rollup
            WHERE id >= {}
            LIMIT {}
        '''
        query = q.format(start_index, limit)
        rows = self.fetchall(query, self.conn)
        d = []
        for row in rows:
            dt = epoch_to_date(row[3]).strftime("%Y-%m-%dT%H:%M:%S")
            item = {
                "id": row[0],
                "sensor_id": row[1],
                "type": self.translate_rollup_type(row[2]),
                "ts": dt,
                "max": int_if_possible(row[4]),
                "min": int_if_possible(row[5]),
                "sum": int_if_possible(row[6]),
                "count": int_if_possible(row[7]),
                "avg": int_if_possible(row[8])
            }
            d.append(item)
        return d

    def data_delete(self, sensor_name, start_date, end_date, group_by=None, function=None):
        raise ValueError("Not implemented")


if __name__ == "__main__":
    db = Datastore("./telemetry.db3", create=True)
    sensor_name = "test"
    t = Timeseries(name=sensor_name)
    now = datetime(2020, 1, 1)
    for _ in range(96 * 10):
        t.add_data_point(now, 1)  # new data point
        now += timedelta(minutes=15)  # next timestamp
    db.data_create(t)  # persist time series
    t2 = db.data_read_interval(sensor_name, "2020-01-01", "2021-01-01")
    t3 = db.data_read_rollup(sensor_name, "2020-01-01", "2021-01-01", "day", "sum")
    db.close()
