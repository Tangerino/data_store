import sys
from calendar import timegm
from datetime import datetime
from time import gmtime

from paths import get_path_log

MUST_QUIT = False
_task_name = ""  # if used in different process can have its own name in the log


def log_init(task_name):
    global _task_name
    _task_name = task_name


def log(message):
    try:
        s = "{} - [{}] - {}".format(datetime.now(), _task_name, message)
        print(s)
        fn = "{}/eds.log".format(get_path_log())
        try:
            with open(fn, 'a') as the_file:
                the_file.write(s + '\n')
        except Exception as e:
            if sys.platform != "darwin":
                print("ERROR - LOG - {}".format(e))
    except Exception as e:
        print(str(e))


def epoch_to_date(epoch):
    tm = gmtime(epoch)
    dt = datetime(tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec)
    return dt


def date_to_epoch(date_time):
    tt = date_time.timetuple()
    epoch = timegm(tt)
    return epoch


def int_if_possible(number):
    i = int(number)
    if i == number:
        return i
    return number
