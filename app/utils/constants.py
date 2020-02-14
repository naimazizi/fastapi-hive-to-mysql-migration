class ReturnCode(object):
    SUCCESS = "0000"
    ERROR_IN_HIVE = "0001"
    ERROR_IN_DB = "0002"
    FILE_NOT_FOUND = "0003"
    BAD_REQUEST = "0004"
    TASK_IS_EXISTS = "0005"
    UNKNOWN_ERROR = "9999"