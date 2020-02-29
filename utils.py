import pickle
import base64

STATUS_TOKEN = "STATUS"
TASKSET_TOKEN = "TASKSET"
CLOSE_TOKEN = "CLOSE"
SETUP_TOKEN = "SETUP"
WORK_TOKEN = "WORK"
RESULT_TOKEN = "RESULT"

TASKDEF_PARAM = "TASKDEF"
TASKLIST_PARAM = "TASKLIST"
CLIENTID_PARAM = "CLIENTID"
TASK_PARAM = "TASK"
RESULT_PARAM = "RESULT"
RESULTLIST_PARAM = "RESULTLIST"
WORKERCOUNT_PARAM = "WORKERCOUNT"
QUEUESIZE_PARAM = "QUEUESIZE"

class TaskDef:
    """Information for a task set that is common among all tasks."""
    def __init__(self, source_file, other_files, function):
        """Simply fills in the values in this class with initial values."""
        self.source_file = source_file
        self.other_files = other_files
        self.function = function

def pack(obj):
    """Converts an object to a UTF8-encoded base64-encoded pickle."""
    return base64.b64encode(pickle.dumps(obj)).decode("utf-8")

def unpack(obj_string):
    """Converts a UTF8-encoded base64-encoded pickle back to the original object."""
    return pickle.loads(base64.b64decode(obj_string.encode("utf-8")))

def buildParameter(name, value):
    """Constructs a named parameter in our syntax of NAME:VALUE."""
    return '{n}:{v}'.format(n=name, v=value)

def parseParameter(param):
    """Parses a named parameter in our syntax of NAME:VALUE into a 2-element list [name, value]."""
    return param.split(':')