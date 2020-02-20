import sys

if sys.platform == "darwin":
    PATH_LOG = "/var/eds/log"
    PATH_CONFIG = "/etc/eds/eds.json"
else:
    PATH_LOG = "/var/eds/log"
    PATH_CONFIG = "/etc/eds/eds.json"


def get_path_config():
    return PATH_CONFIG


def get_path_log():
    return PATH_LOG

