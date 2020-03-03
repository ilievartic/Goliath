from .utils import *
from .exceptions import *
import sys
import asyncio
import queue
import os
import random
import shutil
import signal
import time
import re
import subprocess
import copy
import pkg_resources
import pip
import modulefinder
import pkgutil

class Lieutenant:

    def __init__(self, hostname, port, num_workers=None):
        self.hostname = hostname 
        self.port = port

        """Counts the number of running workers under this lieutenant."""
        self.num_workers = num_workers if num_workers is not None else max(os.cpu_count() - 2, 1)

        """Contains all seen clients like { int(client_id): (asyncio.StreamReader(reader), asyncio.StreamWriter(writer)), ... }."""
        self.clients = {}

        """Contains all a count of requested tasks for all seen clients like { int(client_id): int(num_tasks), ... }."""
        self.num_tasks = {}

        """Contains all results of tasks like { int(client_id): [ (int(task_id), object(result)), ... ], ... }."""
        self.results = {}

        """Contains all done conditions of seen clients like { int(client_id): asyncio.Condition(done_condition), ... }."""
        self.client_done_cond = {}

        """Contains all tasks yet to be completed by this lieutenant."""
        self.task_list = []

        self.closing = False

        self.task_condition = None

        asyncio.run(self.start())

    # Will break on custom modules not defined by filenames
    def getPipDependencies(self, source_file, dependent_files, client_id):
        text = None
        with open(str(client_id) + "/" + source_file, "r") as f:
            text = f.read()
        m = re.findall(IMPORT_REGEX, text)
        potential_packages = []
        for match in m:
            potential_packages.append(match[3])

        local_packages = []
        pip_packages = []
        for i in range(len(potential_packages)):
            potential_package = potential_packages[i]
            if (potential_package + ".py" in dependent_files):
                local_packages.append(potential_package)
            else:
                pip_packages.append(potential_package)

        results = pip_packages
        for local_package in local_packages:
            files = copy.deepcopy(dependent_files)
            files.remove(local_package)
            results.extend(self.getPipDependencies(local_package + ".py", files, client_id))
        
        final_results = []
        for result in results:
            if result not in local_packages:
                final_results.append(result)
        
        return final_results

    # This function from https://stackoverflow.com/questions/12332975/installing-python-module-within-code
    def installPackage(self, package):
        subprocess.call([sys.executable, "-m", "pip", "-qqq", "install", package], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def configureClientFolder(self, task_def, client_id):
        """Creates a client directory and writes all of the required files to that directory."""
        shutil.rmtree(str(client_id), ignore_errors=True)
        os.mkdir(str(client_id))
        file_dict = task_def[1]
        directory_prefix = str(client_id) + "/"
        for filename, contents in file_dict.items():
            with open(directory_prefix + filename, "wb") as f:
                f.write(contents)

        finder = modulefinder.ModuleFinder()
        finder.run_script(directory_prefix + task_def[0])
        installed = list({pkg.key for pkg in pkg_resources.working_set})
        installed.append('commander')
        installed.extend(sys.builtin_module_names)
        installed.extend([x.name for x in pkgutil.iter_modules()])
        imports = None
        with open(directory_prefix + task_def[0], "r") as src:
            imports = "\n".join([line for line in src.readlines() if 'import' in line])
        for package in finder.modules:
            if package not in installed and package in imports:
                self.installPackage(package)

    def serveBadRequest(self, request):
        """Generates a response for a malformed request."""
        return [request[0], "!"]

    def serveStatusRequest(self, request):
        """Generates a response for a status request."""
        return [STATUS_TOKEN, buildParameter(WORKERCOUNT_PARAM, self.num_workers), buildParameter(QUEUESIZE_PARAM, len(self.task_list)), REPLY_STOP]

    async def serveTasksetRequest(self, request, client_id):
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
            return self.serveBadRequest(request)

        # Configure client 'env' and remove the files from the task def
        task_def = unpack(task_def_pack)
        self.configureClientFolder(task_def, client_id)
        task_def = (task_def[0], None, task_def[2])
        task_def_pack = pack(task_def)
        
        # Add all provided tasks to the queue
        for task in task_list:
            self.task_list.append((client_id, task_def_pack, task))
        await self.task_condition.acquire()
        self.task_condition.notify_all()
        self.task_condition.release()

        # Configure variables that will be used to manage monitor the progress of this request
        self.num_tasks[client_id] = len(task_list)
        if (client_id not in self.client_done_cond.keys()):
            self.client_done_cond[client_id] = asyncio.Condition()

        return None

    def serveCloseRequest(self, request):
        # TODO: Handle the closing of the commander
        return [CLOSE_TOKEN, REPLY_STOP]

    async def commanderCallback(self, reader, writer):
        """Determine what to do when a commander connects to this lieutenant."""
        print('callback')
        # Add data for this new client (after choosing a new client ID)
        client_id = len(self.clients)
        self.results[client_id] = []
        self.num_tasks[client_id] = float('inf')
        self.clients[client_id] = (reader, writer)
        
        # Request loop
        while True:
            stop = False
            # Read request from the client
            var_string = await readlineInfinite(reader)
            if (var_string is None or var_string == "" or len(var_string) == 0):
                continue
            request = parseMessage(var_string)
            response = None
            # Ensure the request is well-formated and serve the corresponding task
            if (request[-1] == REQUEST_STOP):
                if (request[0] == STATUS_TOKEN):
                    response = self.serveStatusRequest(request)
                elif (request[0] == TASKSET_TOKEN):
                    response = await self.serveTasksetRequest(request, client_id)
                elif (request[0] == CLOSE_TOKEN):
                    # The commander wants to close the connection; we acknowledge
                    stop = True
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
                writer.write(response_string.encode('utf-8'))
                await writer.drain()

            if (stop):
                writer.close()
                await writer.wait_closed()
                shutil.rmtree(str(client_id), ignore_errors=True)
                return

    async def loadTaskDef(self, worker, task_def_pack, client_id):
        """Send a request to set up the proper environment to a worker."""

        # Send setup request to worker
        task_str_arr = [SETUP_TOKEN, buildParameter(TASKDEF_PARAM, task_def_pack), buildParameter(CLIENTID_PARAM, pack(client_id)), REQUEST_STOP]
        task_str = buildMessage(task_str_arr)
        worker.stdin.write(task_str.encode('utf-8'))
        await worker.stdin.drain()

        var_string = None
        while (True):
            var_string = await readlineInfinite(worker.stdout)
            if (var_string is None or var_string == "" or len(var_string) == 0):
                continue
            else:
                break
        response = parseMessage(var_string)

        if (response[0] != SETUP_TOKEN or response[-1] != REPLY_STOP):
            raise BadReplyException("Expected empty setup reply")

    async def execTask(self, worker, task, client_id):
        """Execute a task on the worker using an environment specified by client_id. Add the result to that clients' results list."""

        # Send task to worker
        task_str_arr = [WORK_TOKEN, buildParameter(TASK_PARAM, pack(task)), buildParameter(CLIENTID_PARAM, pack(client_id)), REQUEST_STOP]
        task_str = buildMessage(task_str_arr)
        worker.stdin.write(task_str.encode('utf-8'))
        await worker.stdin.drain()

        worker.send_signal(signal.SIGINT)
        # Read response from worker
        var_string = None
        while True:
            var_string = await readlineInfinite(worker.stdout)
            if (var_string is None or var_string == "" or len(var_string) == 0):
                continue
            else:
                break
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
        self.results[client_id].append((task_id, unpack(result)))

    async def runWorker(self):
        worker = await asyncio.create_subprocess_shell(sys.executable + " -m goliath.worker", stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
        loaded_task_defs = []
        while not self.closing:
            # Pull a task off the queue
            client_id, task_def_pack, task = (None, None, None)
            if (len(self.task_list) == 0):
                await self.task_condition.acquire()
                await self.task_condition.wait()
                if (len(self.task_list) == 0):
                    self.task_condition.release()
                    continue

                client_id, task_def_pack, task = self.task_list.pop()
                self.task_condition.release()
            else:
                client_id, task_def_pack, task = self.task_list.pop()

            # Ensure the environment for the task has been loaded
            if (task_def_pack, client_id) not in loaded_task_defs:
                await self.loadTaskDef(worker, task_def_pack, client_id)
                loaded_task_defs.append((task_def_pack, client_id))
            
            # Execute the task and put its result in the client's list
            await self.execTask(worker, task, client_id)
        
        worker.stdout = subprocess.DEVNULL
        worker.stderr = subprocess.DEVNULL
        worker.send_signal(signal.SIGTERM)
        self.num_workers -= 1

    async def startWorkers(self):
        """Spin up all of the worker processes and start tasks to feed tasks to the workers."""
        for _ in range(self.num_workers):
            asyncio.create_task(self.runWorker())

    async def responseLoop(self):
        """Continuously check if a client's work has been completed. If it has, send a response."""
        while True:
            for client_id in self.clients.keys():
                if len(self.results[client_id]) == self.num_tasks[client_id]:
                    # Build the response string
                    response_str = buildMessage([TASKSET_TOKEN, buildParameter(RESULTLIST_PARAM, pack(self.results[client_id])), REPLY_STOP])
                    reader, writer = self.clients[client_id]

                    # Send the response
                    writer.write(response_str.encode('utf-8'))
                    await writer.drain()

                    # Do some cleanup
                    self.results[client_id] = []
                    self.num_tasks[client_id] = -1
                    # del self.task_condition[client_id]

                    # Wake up the callback in charge of handling this request
                    await self.client_done_cond[client_id].acquire()
                    self.client_done_cond[client_id].notify_all()
                    self.client_done_cond[client_id].release()

            await asyncio.sleep(0)

    async def killWorkers(self):
        for client_id in self.clients:
            await self.client_done_cond[client_id].acquire()
            self.client_done_cond[client_id].notify_all()
            self.client_done_cond[client_id].release()
        await self.task_condition.acquire()
        self.task_condition.notify_all()
        self.task_condition.release()

        while (self.num_workers > 0):
            await asyncio.sleep(1)
        sys.stdin = None
        sys.stderr = None
        exit(1)

    def close(self):
        self.closing = True
        asyncio.create_task(self.killWorkers())

    async def start(self):
        """Start the server and the workers."""
        asyncio.get_event_loop().add_signal_handler(signal.SIGINT, self.close)
        # Spin up the workers
        self.task_condition = asyncio.Condition()
        await self.startWorkers()
        asyncio.create_task(self.responseLoop())

        # Start listening for commander requests
        server = await asyncio.start_server(self.commanderCallback, self.hostname, self.port, start_serving=False)


        async with server:
            await server.serve_forever()

if __name__ == "__main__":
    """Send a hostname, port, and worker count, and run a lieutenant."""
    if (len(sys.argv) < 3):
        print("Usage: python3 lieutenant.py <hostname> <port> [num_workers]")
        exit(1)
    Lieutenant(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]) if len(sys.argv) >= 3 else None)
