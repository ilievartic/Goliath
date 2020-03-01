from utils import *
import asyncio
import time
import sys

class Commander:
    def __init__(self, lieutenants=None):
        """Initializes object."""

        """Contains all lieutenants like { int(lieutenant_id): (asyncio.StreamReader(reader), asyncio.StreamWriter(writer)), ... }."""
        self.lieutenants = lieutenants
    
    async def connect(self, lieutenants):
        connections = [await asyncio.open_connection(host, port) for host, port in lieutenants]
        ids = range(len(connections))
        self.lieutenants = dict(zip(ids, connections))
    
    async def pollLieutenantStatuses(self):
        """Poll each of the lieutenants to determine their status."""
        worker_counts = {}
        queue_sizes = {}
        # print(self.lieutenants)
        for lieutenant_id, (reader, writer) in self.lieutenants.items():
            writer.write(buildMessage([STATUS_TOKEN, REQUEST_STOP]).encode('utf-8'))
            await writer.drain()
            response = parseMessage((await reader.readline()).decode('utf-8').strip())
            if response[-1] == REPLY_STOP:
                if response[0] == STATUS_TOKEN:
                    for param in response[1:-1]:
                        key, value = parseParameter(param)
                        if key == WORKERCOUNT_PARAM:
                            worker_counts[lieutenant_id] = int(value)
                        elif key == QUEUESIZE_PARAM:
                            queue_sizes[lieutenant_id] = int(value)
                        else:
                            # TODO: Unexpected parameter
                            pass
                else:
                    # TODO: Unexpected token (only expected STATUS_TOKEN)
                    pass
            else:
                # TODO: Unexpected stop character (only expected a reply)
                pass
        return worker_counts, queue_sizes

    async def sendTasksToLieutenant(self, lieutenant_id, task_def_pack, args_pack):
        """Assign the lieutenant to work the given task set."""
        message = buildMessage([
            TASKSET_TOKEN,
            buildParameter(TASKDEF_PARAM, task_def_pack),
            buildParameter(TASKLIST_PARAM, args_pack),
            REQUEST_STOP
        ])
        _, writer = self.lieutenants[lieutenant_id]
        writer.write(message.encode('utf-8'))
        await writer.drain()

    async def readLieutenantResponse(self, lieutenant_id):
        reader, writer = self.lieutenants[lieutenant_id]
        response = parseMessage((await reader.readline()).decode('utf-8').strip())
        if response[-1] == REPLY_STOP:
            if response[0] == TASKSET_TOKEN:
                for param in response[1:-1]:
                    key, value = parseParameter(param)
                    if key == RESULTLIST_PARAM:
                        results[lieutenant_id] = unpack(value)
                    else:
                        # TODO: Unexpected parameter
                        pass
            else:
                # TODO: Unexpected token (only expected TASKSET_TOKEN)
                pass
        else:
            # TODO: Unexpected stop character (only expected a reply)
            pass
        return response

    async def distributeTasksets(self, task_def_pack, args_pack):
        """Given the packed generic task definition, compute the distribution of tasks among lieutenants and send those tasks."""
        worker_counts, queue_sizes = await self.pollLieutenantStatuses()
        num_workers = sum(worker_counts.values())
        num_tasks = len(args_pack)
        contributions = {}
        # Calculate a proportional number of tasks for each lieutenant
        proportional_contribution = dict([(l, int(worker_counts[l] / num_workers * num_tasks)) for l in self.lieutenants])

        for l_id in self.lieutenants:
            if proportional_contribution[l_id] > num_tasks:
                contributions[l_id] = num_tasks
                num_tasks = 0
            else:
                contributions[l_id] = proportional_contribution[l_id]
                num_tasks -= proportional_contribution[l_id]
        
        for l_id, num_tasks in contributions.items():
            self.sendTasksToLieutenant(l_id, task_def_pack, args_pack[:num_tasks])
            args_pack = args_pack[num_tasks:]

        # Sort the list of tuples by the first tuple element, which in this case is the task ID
        results = sorted([await self.readLieutenantResponse(l) for l in self.lieutenants])
        return list(zip(*results)[1]) if results else []
        
    def run(self, function, args, filenames):
        """
        Parameters
        ----------
        function: the name of the function to run
        args: a list of dictionaries. Each dictionary is associated with the parameters of a different task. Each K-V entry in the dictionary represents a named parameter and value example: [ { 'foo': 100, 'bar': 200 } ]
        filenames: a list of necessary files to include, with the source file first
        """
        return asyncio.run(self.asyncRun(function, args, filenames))
        
    async def asyncRun(self, function, args, filenames):
        await self.connect(self.lieutenants)
        file_contents = {}
        for filename in filenames:
            with open(filename, 'rb') as f:
                file_contents[filename] = f.read()
        task_def = (filenames[0], file_contents, function)
        return await self.distributeTasksets(pack(task_def), pack(args))