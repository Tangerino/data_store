from calendar import timegm
from datetime import datetime
from time import gmtime

from dateutil.parser import parse

import constants
from util import epoch_to_date, int_if_possible

TS = constants.TS_TS
VALUE = constants.TS_VALUE
STATUS = constants.TS_STATUS


class Timeseries:
    def __init__(self, name=None):
        self.data_points = list()
        self.name = name

    def to_epoch(self, date_value):
        if isinstance(date_value, str):
            # mas pode ser um numero
            try:
                time_stamp = int(date_value)
                return time_stamp
            except:
                pass
            dt = parse(date_value)
            tt = dt.timetuple()
            time_stamp = timegm(tt)
        elif isinstance(date_value, datetime):
            tt = date_value.timetuple()
            time_stamp = timegm(tt)
        elif isinstance(date_value, int):
            time_stamp = date_value
        elif isinstance(date_value, float):
            time_stamp = int(date_value)
        else:
            time_stamp = None
        return time_stamp

    def add_data_point(self, time_stamp, value, status=constants.DP_STATUS_OK):
        epoch = self.to_epoch(time_stamp)
        if epoch is None:
            raise Exception("Invalid time stamp type")
        dp = [epoch, value, status]
        self.data_points.append(dp)

    def sort(self):
        pass

    def __repr__(self):
        body = ""
        for dp in self.data_points:
            if dp[TS] > constants.MAX_EPOCH:
                tm = gmtime(dp[TS] / 1000)
                msec = dp[TS] % 1000
            else:
                tm = gmtime(dp[TS])
                msec = None
            dt = datetime(tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec)
            if msec is None:
                entry = "[{}, {}, {}]".format(dt, dp[VALUE], dp[STATUS])
            else:
                entry = "[{}.{:03}, {}, {}]".format(dt, msec, dp[VALUE], dp[STATUS])
            body += entry
        obj = "Timeseries(%s,%s)" % (self.name, body)
        return obj

    def to_dict(self):
        o = list()
        for dp in self.data_points:
            dt = epoch_to_date(dp[TS]).strftime("%Y-%m-%dT%H:%M:%S")
            item = [dt, int_if_possible(dp[VALUE]), dp[STATUS]]
            o.append(item)
        return o
