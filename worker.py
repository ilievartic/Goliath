from utils import *
import sys
import asyncio
import os

class Worker:
    def __init__(self):
        self.reader = None
        self.writer = None

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