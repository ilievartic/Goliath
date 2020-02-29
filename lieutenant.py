from .utils import *
import sys
import asyncio
import queue

STATUS_TOKEN = "STATUS"
TASKSET_TOKEN = "TASKSET"
CLOSE_TOKEN = "CLOSE"
SETUP_TOKEN = "SETUP"
WORK_TOKEN = "WORK"

TASKDEF_PARAM = "TASKDEF"
TASKLIST_PARAM = "TASKLIST"

clients = {}
results = []
task_queue = queue.Queue()


async def serveBadRequest(request):
    return [request[0], "!"]

async def serveStatusRequest(request):
    return [str(len(workers)), str(len(task_queue)), "."]

# Request = [TASKSET_TOKEN, packaged_taskset, packaged_task_list, "?"]
async def serveTasksetRequest(request, client_id):
        
    task_def = None
    task_list = None
    for param in request[1:-1]:
        name, val = param.split(":")
        if (name == TASKDEF_PARAM):
            task_def = unpack(val)
        elif (name == TASKLIST_PARAM):
            task_list = unpack(val)
        else:
            return serveBadRequest(request)
    
    if (not task_def or not task_list):
        return serveBadRequest(request)
    
    for task in task_list:
        task_queue.put((client_id, task_def, task))

async def serveCloseRequset(request):
    return [CLOSE_TOKEN, "."]

def sendResponse(response, writer):
    string_response = " ".join(response)

async def commanderCallback(reader, writer):
    client_id = len(clients) + 1
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
        writer.write(response_string)

async def loadTaskDef(worker, task_def, client_id):
    task_str = [WORK_TOKEN]

async def execTask(worker, task, client_id):
    pass

async def runWorker(worker):
    loaded_task_defs = []
    while True:
        task = await task_queue.get()
        client_id = task[0]
        task_def = task[1]
        task = task[2]

        if (task_def not in loaded_task_defs):
            await loadTaskDef(worker, task_def, client_id)
            loaded_task_defs.append(task_def)
        
        await execTask(worker, task, client_id)

def startWorkers(num_workers):
    for _ in range(num_workers):
        worker = asyncio.create_subprocess_exec(program="python3", args=["worker.py"], stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
        asyncio.create_task(runWorker(worker))

async def main(hostname, port, num_workers):
    # Spin up workers
    startWorkers(num_workers)

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