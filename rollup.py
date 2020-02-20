import sys
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta

from constants import JOB_TYPE_YEAR, JOB_TYPE_DAY, JOB_TYPE_HOUR, JOB_TYPE_MONTH
from util import log, date_to_epoch, epoch_to_date


def get_eoi_query(sensor_id, eoi, sd, ed):
    q = '''
        REPLACE INTO rollup (sensor_id,
                             type,
                             ts,
                             vsum,
                             vmax,
                             vmin,
                             vcount,
                             vavg)
        SELECT 
            {} "id",
            0 "type",
            strftime('%s', strftime('%Y-%m-%d %H:00:00', ts - {}, 'unixepoch')) "ts",
            IFNULL(SUM(value),0) "sum",
            IFNULL(MAX(value),0) "max",
            IFNULL(MIN(value),0) "min",
            IFNULL(COUNT(value),0) "count",
            CASE
            WHEN COUNT(value) > 0 THEN SUM(value)/COUNT(value)
            ELSE 0
            END "avg"
        FROM series
        WHERE sensor_id = {}
        AND status >= 0
        {}
        GROUP BY 3
    '''
    if eoi:
        seconds = "1"
        t = '''
            AND ts >= {} 
            AND ts <= {} 
        '''
    else:
        seconds = "0"
        t = '''
            AND ts >= {} 
            AND ts <= {} 
        '''
    time_range = t.format(sd, ed)
    query = q.format(sensor_id, seconds, sensor_id, time_range)
    return query


def rollup_job_hour(conn, sensor_id, jobs, verbose=False, dry_run=False):
    sd = int(sys.float_info.max - 1)
    ed = -sd
    for k, _ in jobs.items():
        if k < sd:
            sd = k
        if k > ed:
            ed = k
    if ed - sd < 3600:
        ed = sd + 3600
    query = get_eoi_query(sensor_id, False, sd, ed)
    if verbose > 1:
        log("rollup_job_hour - {}".format(query))
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        s = "ERROR: rollup_job_hour - {} -{}".format(e, query)
        raise Exception(s)


def get_dmy_query(sensor_id, job_type, source_type, sd, ed):
    if job_type == JOB_TYPE_DAY:
        dt_fmt = "%Y-%m-%d 00:00:00"
    elif job_type == JOB_TYPE_MONTH:
        dt_fmt = "%Y-%m-01 00:00:00"
    else:
        dt_fmt = "%Y-01-01 00:00:00"
    q = '''
        REPLACE INTO rollup (sensor_id, type, ts, vsum, vmax, vmin, vcount, vavg)
        SELECT 
            {}, {},
            strftime('%s',strftime('{}', ts, 'unixepoch')),
            SUM(vsum), 
            MAX(vmax), 
            MIN(vmin), 
            SUM(vcount),
            0
            FROM
                rollup
            WHERE
                sensor_id = {} AND type = {}
                    AND ts >= {}
                    AND ts < {}
            GROUP BY 3
    '''
    query = q.format(sensor_id, job_type, dt_fmt, sensor_id, source_type, sd, ed)
    return query


def rollup_job_dmy(conn, sensor_id, jobs, job_type, source_type, verbose=False, dry_run=False):
    sd = sys.float_info.max
    ed = sys.float_info.min
    for k, _ in jobs.items():
        if k < sd:
            sd = k
        if k > ed:
            ed = k
    if len(jobs) == 1:
        dt = epoch_to_date(sd)
        if job_type == JOB_TYPE_DAY:
            dt += timedelta(days=1)
        elif job_type == JOB_TYPE_MONTH:
            dt += relativedelta(months=1)
        else:
            dt += relativedelta(months=12)
        ed = date_to_epoch(dt)
    query = get_dmy_query(sensor_id, job_type, source_type, sd, ed)
    if verbose > 1:
        log("rollup_job_dmy - {}".format(query))
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        s = "ERROR: rollup_job_dmy - {} - {}".format(e, query)
        raise Exception(s)


def rollup_job(db, job_type, sensor_id, jobs, verbose=False, dry_run=False):
    if dry_run:
        return
    try:
        if job_type == JOB_TYPE_HOUR:
            rollup_job_hour(db, sensor_id, jobs, verbose=verbose, dry_run=dry_run)
        elif job_type == JOB_TYPE_DAY:
            rollup_job_dmy(db, sensor_id, jobs, JOB_TYPE_DAY, JOB_TYPE_HOUR, verbose=verbose, dry_run=dry_run)
        elif job_type == JOB_TYPE_MONTH:
            rollup_job_dmy(db, sensor_id, jobs, JOB_TYPE_MONTH, JOB_TYPE_DAY, verbose=verbose, dry_run=dry_run)
        elif job_type == JOB_TYPE_YEAR:
            rollup_job_dmy(db, sensor_id, jobs, JOB_TYPE_YEAR, JOB_TYPE_MONTH, verbose=verbose, dry_run=dry_run)
        else:
            raise Exception("rollup_job - Invalid job type - {}".format(job_type))
    except Exception as e:
        s = "ERROR - rollup_job - Type: {} - {}".format(job_type, e)
        raise Exception(s)


def rollup(args):
    pass


def reduce_jobs(jobs, job_type):
    new_jobs = dict()
    for ts, _ in jobs.items():
        dt = epoch_to_date(ts)
        if job_type == JOB_TYPE_HOUR:
            dt = datetime(dt.year, dt.month, dt.day)
        elif job_type == JOB_TYPE_DAY:
            dt = datetime(dt.year, dt.month, 1)
        elif job_type == JOB_TYPE_MONTH:
            dt = datetime(dt.year, 1, 1)
        epoch = date_to_epoch(dt)
        new_jobs[epoch] = 1
    return new_jobs


def rollup_sensor(conn, sensor_id, jobs, verbose=False, dry_run=False):
    rollup_job(conn, JOB_TYPE_HOUR, sensor_id, jobs, verbose=verbose, dry_run=dry_run)
    jobs = reduce_jobs(jobs, JOB_TYPE_HOUR)
    rollup_job(conn, JOB_TYPE_DAY, sensor_id, jobs, verbose=verbose, dry_run=dry_run)
    jobs = reduce_jobs(jobs, JOB_TYPE_DAY)
    rollup_job(conn, JOB_TYPE_MONTH, sensor_id, jobs, verbose=verbose, dry_run=dry_run)
    jobs = reduce_jobs(jobs, JOB_TYPE_MONTH)
    rollup_job(conn, JOB_TYPE_YEAR, sensor_id, jobs, verbose=verbose, dry_run=dry_run)
