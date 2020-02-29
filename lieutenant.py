from .utils import *
import sys
import asyncio
import queue

clients = {}
num_tasks = {}
results = {}
workers = 0
task_queue = queue.Queue()

async def serveBadRequest(request):
    return [request[0], "!"]

async def serveStatusRequest(request):
    return [STATUS_TOKEN, buildParameter(WORKERCOUNT_PARAM, str(workers)), buildParameter(QUEUESIZE_PARAM, str(len(task_queue))), "."]

# Request = [TASKSET_TOKEN, packaged_taskset, packaged_task_list, "?"]
async def serveTasksetRequest(request, client_id):
        
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
        return serveBadRequest(request)
    
    for task in task_list:
        task_queue.put((client_id, task_def_pack, task))

    num_tasks[client_id] = len(task_list)

async def serveCloseRequset(request):
    return [CLOSE_TOKEN, "."]

def sendResponse(response, writer):
    string_response = " ".join(response)

async def commanderCallback(reader, writer):
    client_id = len(clients)
    results[client_id] = []
    clients[(reader, writer)] = client_id
    
    # Request loop
    while True:
        request = await reader.readline().strip().split(" ")
        response = None

        if (request[-1] == "?"):
            if (request[0] == STATUS_TOKEN):
                response = await serveStatusRequest(request)
            elif (request[0] == TASKSET_TOKEN):
                response = await serveTasksetRequest(request, client_id)
            elif (request[0] == CLOSE_TOKEN):
                response = await serveCloseRequset(request)
            else:
                response = await serveBadRequest(request)
        else:
            response = await serveBadRequest(request)

        response_string = " ".join(response) + "\n"
        await writer.write(response_string)

async def loadTaskDef(worker, task_def_pack, client_id):
    task_str_arr = [SETUP_TOKEN, buildParameter(TASKDEF_PARAM, task_def_pack), buildParameter(CLIENTID_PARAM, pack(client_id)) , "?"]
    task_str = " ".join(task_str_arr) + "\n"
    worker.stdin.write(task_str)
    await worker.stdin.drain()

async def execTask(worker, task, client_id):
    task_str_arr = [WORK_TOKEN, buildParameter(TASK_PARAM, pack(task)), buildParameter(CLIENTID_PARAM, pack(client_id)), "?"]
    task_str = " ".join(task_str_arr) + "\n"
    worker.stdin.write(task_str)
    await worker.stdin.drain()

    # TODO: Get response from worker
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
    loaded_task_defs = []
    while True:
        task = await task_queue.get()
        client_id = task[0]
        task_def_pack = task[1]
        task = task[2]

        if (task_def_pack not in loaded_task_defs):
            await loadTaskDef(worker, task_def_pack, client_id)
            loaded_task_defs.append(task_def_pack)
        
        await execTask(worker, task, client_id)

def startWorkers(num_workers):
    for _ in range(num_workers):
        worker = asyncio.create_subprocess_exec(program="python3", args=["worker.py"], stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
        asyncio.create_task(runWorker(worker))

async def responseLoop():
    while True:
        for client_id in clients.values():
            if len(results[client_id] == num_tasks[client_id]):
                # TODO: Send response
                response_str_array = [RESULT_TOKEN, buildParameter(RESULTLIST_PARAM, pack(results[client_id])), "."]
        await asyncio.sleep(0)

async def main(hostname, port, num_workers):
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
    asyncio.run(main(sys.argv[1], sys.argv[2], sys.argv[3]))