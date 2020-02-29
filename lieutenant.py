from utils import *
from exceptions import *
import sys
import asyncio
import queue
import os


class Lieutenant:
    def __init__(self, hostname, port, num_workers=max(os.cpu_count() - 2, 1)):
        self.hostname = hostname 
        self.port = port

        """Counts the number of running workers under this lieutenant."""
        self.num_workers = num_workers

        """Contains all seen clients like { int(client_id): (asyncio.StreamReader(reader), asyncio.StreamWriter(writer)), ... }."""
        self.clients = {}

        """Contains all a count of requested tasks for all seen clients like { int(client_id): int(num_tasks), ... }."""
        self.num_tasks = {}

        """Contains all results of tasks like { int(client_id): [ (int(task_id), object(result)), ... ], ... }."""
        self.results = {}

        """Contains all done conditions of seen clients like { int(client_id): asyncio.Condition(done_condition), ... }."""
        self.client_done_cond = {}

        """Contains all tasks yet to be completed by this lieutenant."""
        self.task_queue = queue.Queue()

    def configureClientFolder(self, task_def, client_id):
        """Creates a client directory and writes all of the required files to that directory."""
        os.mkdir(str(client_id))
        file_dict = task_def[1]
        directory_prefix = str(client_id) + "/"
        for filename, contents in file_dict.items():
            with open(directory_prefix + filename, "wb") as f:
                f.write(contents)

    def serveBadRequest(self, request):
        """Generates a response for a malformed request."""
        return [request[0], "!"]

    def serveStatusRequest(self, request):
        """Generates a response for a status request."""
        return [STATUS_TOKEN, buildParameter(WORKERCOUNT_PARAM, self.num_workers), buildParameter(QUEUESIZE_PARAM, len(self.task_queue)), REPLY_STOP]

    def serveTasksetRequest(self, request, client_id):
        """Sets up the task queue to handle the taskset request and returns None, or returns a response if unable to do so."""

        # Extract expected parameters from the request
        task_def_pack = None
        task_list = None
        for param in request[1:-1]:
            name, val = parseParameter(param)
            if (name == TASKDEF_PARAM):
                task_def_pack = val
            elif (name == TASKLIST_PARAM):
                task_list = unpack(val)
            else:
                # If there is an unexpected parameter, treat as a malformed request
                # NOTE: Should we ignore unexpected parameters?
                return self.serveBadRequest(request)
        
        if not task_def_pack or not task_list:
            return self.serveBadRequest(request, client_id)

        # Configure client 'env' and remove the files from the task def
        task_def = unpack(task_def_pack)
        self.configureClientFolder(task_def)
        task_def[1] = None
        task_def_pack = pack(task_def)
        
        # Add all provided tasks to the queue
        for task in task_list:
            self.task_queue.put((client_id, task_def_pack, task))

        # Configure variables that will be used to manage monitor the progress of this request
        self.num_tasks[client_id] = len(task_list)
        self.client_done_cond[client_id] = asyncio.Condition()

        return None

    def serveCloseRequest(self, request):
        # TODO: Handle the closing of the commander
        return [CLOSE_TOKEN, REPLY_STOP]

    async def commanderCallback(self, reader, writer):
        """Determine what to do when a commander connects to this lieutenant."""

        # Add data for this new client (after choosing a new client ID)
        client_id = len(self.clients)
        self.results[client_id] = []
        self.clients[client_id] = (reader, writer)
        
        # Request loop
        while True:
            # Read request from the client
            var_string = (await reader.readline()).decode('utf-8').strip()
            if (var_string is None or var_string == "" or len(var_string) == 0):
                continue
            request = parseMessage(var_string)
            response = None
            # Ensure the request is well-formated and serve the corresponding task
            if (request[-1] == REQUEST_STOP):
                if (request[0] == STATUS_TOKEN):
                    response = self.serveStatusRequest(request)
                elif (request[0] == TASKSET_TOKEN):
                    response = self.serveTasksetRequest(request, client_id)
                elif (request[0] == CLOSE_TOKEN):
                    # The commander wants to close the connection; we acknowledge
                    response = self.serveCloseRequest(request)
                else:
                    # We didn't see a request token that we recognized, 
                    response = self.serveBadRequest(request)
            else:
                # If the message doesn't end with a '?' token, it's not a request
                # (Lieutenant only expects requests from the commander, we will ignore)
                raise BadReplyException("The commander sent a bad request")

            if response is None:
                # This means the request was well-formatted and the tasks were put on the queue
                # (serveTasksetRequest returns None as a response when it is successful)
                # (There is an action queued for later that will reply)
                await self.client_done_cond[client_id].acquire()
                await self.client_done_cond[client_id].wait()
                self.client_done_cond[client_id].release()
            else:
                # serveTasksetRequest only returns a response when there was a problem
                # (we should send it now, since there is no action queued to reply later)
                response_string = buildMessage(response)
                writer.write(response_string)
                await writer.drain()

    async def loadTaskDef(self, worker, task_def_pack, client_id):
        """Send a request to set up the proper environment to a worker."""

        # Send setup request to worker
        task_str_arr = [SETUP_TOKEN, buildParameter(TASKDEF_PARAM, task_def_pack), buildParameter(CLIENTID_PARAM, pack(client_id)), REQUEST_STOP]
        task_str = buildMessage(task_str_arr)
        worker.stdin.write(task_str)
        await worker.stdin.drain()

        var_string = None
        while (True):
            var_string = (await worker.stdout.readline()).decode('utf-8').strip()
            if (var_string is None or var_string == "" or len(var_string) == 0):
                continue
        response = parseMessage(var_string)

        if (response[0] != SETUP_TOKEN or response[-1] != REPLY_STOP):
            raise BadReplyException("Expected empty setup reply")

    async def execTask(self, worker, task, client_id):
        """Execute a task on the worker using an environment specified by client_id. Add the result to that clients' results list."""

        # Send task to worker
        task_str_arr = [WORK_TOKEN, buildParameter(TASK_PARAM, pack(task)), buildParameter(CLIENTID_PARAM, pack(client_id)), REQUEST_STOP]
        task_str = buildMessage(task_str_arr)
        worker.stdin.write(task_str)
        await worker.stdin.drain()

        # Read response from worker
        var_string = None
        while True:
            var_string = (await worker.stdout.readline()).decode('utf-8').strip()
            if (var_string is None or var_string == "" or len(var_string) == 0):
                continue
        response = parseMessage(var_string)
        if (response[0] != WORK_TOKEN or response[-1] != REPLY_STOP):
            raise BadReplyException("Worker response has the wrong format")
        
        result = None
        for param in response[1:-1]:
            name, val = parseParameter(param)
            if (name == RESULT_PARAM):
                result = val

        if result is None:
            raise NoWorkerResult("Worker has no result")
        
        task_id = task[0]
        self.results[client_id].append((task_id, result))

    async def runWorker(self, worker):
        """Feed a worker tasks from the queue and set up an environment if needed."""
        loaded_task_defs = []
        while True:
            # Pull a task off the queue
            client_id, task_def_pack, task = await self.task_queue.get()

            # Ensure the environment for the task has been loaded
            if task_def_pack not in loaded_task_defs:
                await self.loadTaskDef(worker, task_def_pack, client_id)
                loaded_task_defs.append(task_def_pack)
            
            # Execute the task and put its result in the client's list
            await self.execTask(worker, task, client_id)

    def startWorkers(self):
        """Spin up all of the worker processes and start tasks to feed tasks to the workers."""
        for _ in range(self.num_workers):
            worker = asyncio.create_subprocess_exec(program="python3", args=["worker.py"], stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
            asyncio.create_task(self.runWorker(worker))

    async def responseLoop(self):
        """Continuously check if a client's work has been completed. If it has, send a response."""
        while True:
            for client_id in self.clients.keys():
                if len(self.results[client_id] == self.num_tasks[client_id]):
                    # Build the response string
                    response_str = buildMessage([TASKSET_TOKEN, buildParameter(RESULTLIST_PARAM, pack(self.results[client_id])), REPLY_STOP])
                    reader, writer = self.clients[client_id]

                    # Send the response
                    writer.write(response_str)
                    await writer.drain()

                    # Wake up the callback in charge of handling this request
                    await self.client_done_cond[client_id].acquire()
                    self.client_done_cond[client_id].notify_all()
                    self.client_done_cond[client_id].release()

            await asyncio.sleep(0)

    async def start(self):
        """Start the server and the workers."""
        # Spin up the workers
        self.startWorkers()
        asyncio.start_task(self.responseLoop())

        # Start listening for commander requests
        server = await asyncio.start_server(self.commanderCallback, self.hostname, self.port)

        async with server:
            await server.serve_forever()

if __name__ == "__main__":
    """Send a hostname, port, and worker count, and run a lieutenant."""
    if (len(sys.argv) < 4):
        print("Usage: python3 lieutenant.py <hostname> <port> <num_workers>")
        exit(1)
    lieutenant = Lieutenant(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
    asyncio.run(lieutenant.start())