import pickle
import base64
import asyncio

"""Tokens are words that go at the beginning of a message to indicate the topic and parameters."""
STATUS_TOKEN = "STATUS"
TASKSET_TOKEN = "TASKSET"
CLOSE_TOKEN = "CLOSE"
SETUP_TOKEN = "SETUP"
WORK_TOKEN = "WORK"
RESULT_TOKEN = "RESULT"

"""Params are words that go in the middle of a message to label a parameter attached with a colon, e.g. WORKERCOUNT:8."""
TASKDEF_PARAM = "TASKDEF"
TASKLIST_PARAM = "TASKLIST"
CLIENTID_PARAM = "CLIENTID"
TASK_PARAM = "TASK"
RESULT_PARAM = "RESULT"
RESULTLIST_PARAM = "RESULTLIST"
WORKERCOUNT_PARAM = "WORKERCOUNT"
QUEUESIZE_PARAM = "QUEUESIZE"

"""Stops are words that go at the end of a message to indicate the type of the message: request, reply, or error."""
REQUEST_STOP = "?"
REPLY_STOP = "."
ERROR_STOP = "!"

IMPORT_REGEX = r"^((import)|(from)) *([a-z,A-Z,0-9,\-]+)"

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

def buildMessage(words):
    """Constructs a message from a list of words."""
    return ' '.join(words) + '\n'

def parseMessage(message):
    """Parses a message into a list of words."""
    return message.split(' ')

async def readlineInfinite(reader, do_print=False, name=""):
    buffer = b''
    while not reader.at_eof():
        try:
            if (do_print):
                print(name + ' readline')
            bytearr = await reader.readuntil(b'\n')
            buffer += bytearr
            if (do_print):
                print(name + ' ' + str(len(bytearr)))
            return buffer.decode('utf-8').strip() if len(buffer) else None
        except asyncio.exceptions.LimitOverrunError:
            if (do_print):
                print(name + ' readexactly')
            bytearr = await reader.readexactly(1024*64)
            if (do_print):
                print(name + ' ' + str(len(bytearr)))
            buffer += bytearr

    return buffer.decode('utf-8').strip() if len(buffer) else None