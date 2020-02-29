import pickle
import base64

STATUS_TOKEN = "STATUS"
TASKSET_TOKEN = "TASKSET"
CLOSE_TOKEN = "CLOSE"
SETUP_TOKEN = "SETUP"
WORK_TOKEN = "WORK"

TASKDEF_PARAM = "TASKDEF"
TASKLIST_PARAM = "TASKLIST"
CLIENTID_PARAM = "CLIENTID"

class TaskDef:
    def __init__(self, source_file, other_files, function):
        self.source_file = source_file
        self.other_files = other_files
        self.function = function

def pack(obj):
    return base64.b64encode(pickle.dumps(obj)).decode("utf-8")

def unpack(obj_string):
    return pickle.loads(base64.b64decode(obj_string.encode("utf-8")))