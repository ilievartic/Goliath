from utils import *
import sys
import asyncio
import queue
import os

clients = {}
num_tasks = {}
results = {}
client_done_cond = {}
workers = 0
task_queue = queue.Queue()

def configureClientFolder(task_def, client_id):
    # Create a client directory and write all of the files to that directory
    os.mkdir(str(client_id))
    file_dict = task_def[1]
    directory_prefix = str(client_id) + "/"
    for filename, contents in file_dict.items():
        with open(directory_prefix + filename, "wb") as f:
            f.write(contents)

def serveBadRequest(request):
    return [request[0], "!"]

def serveStatusRequest(request):
    return [STATUS_TOKEN, buildParameter(WORKERCOUNT_PARAM, str(workers)), buildParameter(QUEUESIZE_PARAM, str(len(task_queue))), "."]

# Request = [TASKSET_TOKEN, packaged_taskset, packaged_task_list, "?"]
def serveTasksetRequest(request, client_id):
        
    # Extract parameters from the request
    task_def_pack = None
    task_list = None
    for param in request[1:-1]:
        name, val = parseParameter(param)
        if (name == TASKDEF_PARAM):
            task_def_pack = val
        elif (name == TASKLIST_PARAM):
            task_list = unpack(val)
        else:
            return serveBadRequest(request)
    
    if (not task_def_pack or not task_list):
        return serveBadRequest(request, client_id)

    # Configure client 'env' and remove the files from the task def
    task_def = unpack(task_def_pack)
    configureClientFolder(task_def)
    task_def[1] = None
    task_def_pack = pack(task_def)
    
    # Add tasks to the queue
    for task in task_list:
        task_queue.put((client_id, task_def_pack, task))

    # Configure variables that will be used to manage monitor the progress of thsi request
    num_tasks[client_id] = len(task_list)
    client_done_cond[client_id] = asyncio.Condition()

    return None

def serveCloseRequset(request):
    return [CLOSE_TOKEN, "."]

async def commanderCallback(reader, writer):
    """ Invoked when a commander connects to this lieutenant """

    # Set up clinet stuff
    client_id = len(clients)
    results[client_id] = []
    clients[client_id] = (reader, writer)
    
    # Request loop
    while True:
        # Get request
        request = await reader.readline().strip().split(" ")
        response = None

        # Ensure it is well-formated and do the tasks if it is
        if (request[-1] == "?"):
            if (request[0] == STATUS_TOKEN):
                response = serveStatusRequest(request)
            elif (request[0] == TASKSET_TOKEN):
                response = serveTasksetRequest(request, client_id)
            elif (request[0] == CLOSE_TOKEN):
                response = serveCloseRequset(request)
            else:
                response = serveBadRequest(request)
        else:
            response = serveBadRequest(request)

        if (response is None):
            # This means the request was well-formated and the tasks were put on the queue
            await client_done_cond[client_id].acquire()
            await client_done_cond[client_id].wait()
            client_done_cond[client_id].release()
        else:
            # The request was poorly formatted, send an error response
            response_string = " ".join(response) + "\n"
            writer.write(response_string)
            await writer.drain()

async def loadTaskDef(worker, task_def_pack, client_id):
    """ Send a request to set up the proper 'environment' to the worker """

    # Send setup request to worker
    task_str_arr = [SETUP_TOKEN, buildParameter(TASKDEF_PARAM, task_def_pack), buildParameter(CLIENTID_PARAM, pack(client_id)) , "?"]
    task_str = " ".join(task_str_arr) + "\n"
    worker.stdin.write(task_str)
    await worker.stdin.drain()

async def execTask(worker, task, client_id):
    """ Execute a task on the worker using an environment specified by client_id. Add the result to the list of that clients results """

    # Send task to worker
    task_str_arr = [WORK_TOKEN, buildParameter(TASK_PARAM, pack(task)), buildParameter(CLIENTID_PARAM, pack(client_id)), "?"]
    task_str = " ".join(task_str_arr) + "\n"
    worker.stdin.write(task_str)
    await worker.stdin.drain()

    # Get Response from worker
    response = await worker.stdout.readline().strip().split(" ")
    if (response[0] != RESULT_TOKEN):
        # TODO: Handle a bad response from a worker
        pass
    
    result = None
    for param in response[1:-1]:
        name, val = parseParameter(param)
        if (name == RESULT_PARAM):
            result = val
        else:
            # TODO: Handle a bad response from a worker
            pass

    if (not result):
        # TODO: Handle a bad response from a worker
        pass
    
    task_id = task[0]
    results[client_id].append((task_id, result))

async def runWorker(worker):
    """ Feeds the workers tasks from the queue and sets up environments as needed. """
    loaded_task_defs = []
    while True:
        # Pull task off the queue
        task = await task_queue.get()
        client_id = task[0]
        task_def_pack = task[1]
        task = task[2]

        # Ensure the 'environment' for that task has been loaded
        if (task_def_pack not in loaded_task_defs):
            await loadTaskDef(worker, task_def_pack, client_id)
            loaded_task_defs.append(task_def_pack)
        
        # Execute that task and put its result in the list
        await execTask(worker, task, client_id)

def startWorkers(num_workers):
    """ Spin up all of the worker processes, and start tasks to feed those workers tasks """
    for _ in range(num_workers):
        worker = asyncio.create_subprocess_exec(program="python3", args=["worker.py"], stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
        asyncio.create_task(runWorker(worker))

async def responseLoop():
    """ Continuously check if a commander's request has been completed. If it has, send a response. """
    while True:
        for client_id in clients.values():
            if len(results[client_id] == num_tasks[client_id]):
                # Build response string
                response_str_array = [RESULT_TOKEN, buildParameter(RESULTLIST_PARAM, pack(results[client_id])), "."]
                response_str = " ".join(response_str_array) + "\n"
                reader, writer = clients[client_id]

                # Send response
                writer.write(response_str)
                await writer.drain()

                # Wake up the callback in charge of handling this request
                await client_done_cond[client_id].acquire()
                client_done_cond[client_id].notify_all()
                client_done_cond[client_id].release()

        await asyncio.sleep(0)

async def main(hostname, port, num_workers):
    """ Start the workers and server """
    # Spin up workers
    startWorkers(num_workers)
    asyncio.start_task(responseLoop())
    workers = num_workers

    # Start listening for commander requests
    server = await asyncio.start_server(commanderCallback, hostname, port)

    async with server:
        await server.serve_forever()

# Args: hostname, port, num_workers
if __name__ == "__main__":
    num_args = 4
    if (len(sys.argv) < num_args):
        print("Too few args")
        exit(1)
    asyncio.run(main(sys.argv[1], int(sys.argv[2]), int(sys.argv[3])))