from utils import *
import sys
import asyncio
import os
import importlib

class Worker:
    def __init__(self):
        self.reader = None
        self.writer = None
        self.functions = {}
        self.modules = {}

    def serveSetupRequest(self, request):
        task_def = None
        client_id = None
        for param in request[1:-1]:
            name, val = parseParameter(param)
            if (name == TASKDEF_PARAM):
                task_def = unpack(val)
            elif (name == CLIENTID_PARAM):
                client_id = unpack(val)
            else:
                # TODO: Handle bad request
                pass

        if task_def is None or client_id is None:
            # TODO: Handle bad request
            pass

        source_file = task_def[0]
        function = task_def[2]
        module_name = str(client_id) + "." + os.path.splitext(source_file)
        importlib.invalidate_caches()
        self.modules[client_id] = importlib.import_module(module_name, ".")

        response = [SETUP_TOKEN, REPLY_STOP]
        return response

    def serveWorkRequest(self, request):
        task = None
        client_id = None
        for param in request[1:-1]:
            name, val = parseParameter(param)
            if (name == TASK_PARAM):
                task = unpack(val)
            elif (name == CLIENTID_PARAM):
                client_id = unpack(val)
            else:
                # TODO: handle bad request
                pass

        if task is None or client_id is None:
            # TODO: Handle bad request
            pass
        
        tid, args = task
        result = self.functions[client_id](**args)

        response = [WORK_TOKEN, buildParameter(RESULT_PARAM, pack(result))]
        return response

    async def taskExecutionLoop(self):
        while True:
            request = parseMessage(await self.reader.readline().strip())
            response = None

            if (request[-1] == REQUEST_STOP):
                if (request[0] == SETUP_TOKEN):
                    response = self.serveSetupRequest(request)
                elif (request[0] == WORK_TOKEN):
                    response = self.serveWorkRequest(request)
                else:
                    # TODO: Handle bad requests
                    pass
            else:
                # TODO: Handle bad requests
                pass

            response_string = buildMessage(response)
            self.writer.write(response_string)
            await self.writer.drain()

    async def start(self):
        # Reader/writer defintion from https://stackoverflow.com/questions/52089869/how-to-create-asyncio-stream-reader-writer-for-stdin-stdout
        limit = asyncio.streams._DEFAULT_LIMIT
        loop = asyncio.get_event_loop()
        self.reader = asyncio.StreamReader(limit=limit, loop=loop)
        await loop.connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(self.reader, loop=loop), sys.stdin)
        writer_transport, writer_protocol = await loop.connect_write_pipe(
            lambda: asyncio.streams.FlowControlMixin(loop=loop),
            os.fdopen(sys.stdout.fileno(), 'wb'))
        self.writer = asyncio.streams.StreamWriter(writer_transport, writer_protocol, None, loop)

        await taskExecutionLoop()

if __name__ == "__main__":
    """Create and run a worker."""
    worker = Worker()
    asyncio.run(worker.start())