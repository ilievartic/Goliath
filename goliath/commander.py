from .utils import *
import asyncio
import time
import sys

#TODO: Commander sending requests in batches instead of splitting all at once
#TODO: Worker Recycling
#TODO: Stoping on Commander
#TODO: Static and Dynamic Argument Dictionaries
#TODO: Local Support

class Commander:
    def __init__(self, lieutenants=None):
        """Initializes object."""

        """Contains all lieutenants like { int(lieutenant_id): (asyncio.StreamReader(reader), asyncio.StreamWriter(writer)), ... }."""
        self.lieutenants_data = lieutenants
        self.lieutenants = None
    
    async def connect(self, lieutenants):
        connections = [await asyncio.open_connection(host, port) for host, port in lieutenants]
        ids = range(len(connections))
        self.lieutenants = dict(zip(ids, connections))
    
    async def pollLieutenantStatuses(self):
        """Poll each of the lieutenants to determine their status."""
        worker_counts = {}
        queue_sizes = {}
        for lieutenant_id, (reader, writer) in self.lieutenants.items():
            writer.write(buildMessage([STATUS_TOKEN, REQUEST_STOP]).encode('utf-8'))
            await writer.drain()
            response = parseMessage(await readlineInfinite(reader))
            if response[-1] == REPLY_STOP:
                if response[0] == STATUS_TOKEN:
                    for param in response[1:-1]:
                        key, value = parseParameter(param)
                        if key == WORKERCOUNT_PARAM:
                            worker_counts[lieutenant_id] = int(value)
                        elif key == QUEUESIZE_PARAM:
                            queue_sizes[lieutenant_id] = int(value)
                        else:
                            # Unexpected parameter
                            return -1,-1
                else:
                    # Unexpected token (only expected STATUS_TOKEN)
                    return -1,-1
            else:
                # Unexpected stop character (only expected a reply)
                return -1,-1
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
        response = parseMessage(await readlineInfinite(reader))
        results = None
        if response[-1] == REPLY_STOP:
            if response[0] == TASKSET_TOKEN:
                for param in response[1:-1]:
                    key, value = parseParameter(param)
                    if key == RESULTLIST_PARAM:
                        results = unpack(value)
                    else:
                        # TODO: Unexpected parameter
                        results = None
            else:
                # Unexpected token (only expected TASKSET_TOKEN)
                results = None
        else:
            # Unexpected stop character (only expected a reply)
            results = None
        return results

    async def distributeTasksets(self, task_def_pack, args):
        """Given the packed generic task definition, compute the distribution of tasks among lieutenants and send those tasks."""
        await self.connect(self.lieutenants_data)
        new_args = []
        for i in range(0, len(args)):
            new_args.append((i, args[i]))
        args = new_args

        worker_counts, queue_sizes = await self.pollLieutenantStatuses()
        num_workers = sum(worker_counts.values())
        num_tasks = len(args)
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
            await self.sendTasksToLieutenant(l_id, task_def_pack, pack(args[:num_tasks]))
            args = args[num_tasks:]

        # Sort the list of tuples by the first tuple element, which in this case is the task ID
        results = sorted([await self.readLieutenantResponse(l) for l in self.lieutenants])
        actual_results = []
        for result_set in results:
            actual_results.extend(result_set)
        actual_results.sort()
        for lieutenant_id in self.lieutenants:
            reader, writer = self.lieutenants[lieutenant_id]
            message = buildMessage([CLOSE_TOKEN, REQUEST_STOP])
            writer.write(message.encode('utf-8'))
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        self.lieutenants = None
        return [item[1] for item in actual_results] if results else []
        
    def run(self, function, args, filenames):
        """
        Parameters
        ----------
        function: the function to run
        args: a list of dictionaries. Each dictionary is associated with the parameters of a different task. Each K-V entry in the dictionary represents a named parameter and value example: [ { 'foo': 100, 'bar': 200 } ]
        filenames: a list of necessary files to include, with the source file first
        """
        function = function.__name__
        file_contents = {}
        for filename in filenames:
            with open(filename, 'rb') as f:
                file_contents[filename] = f.read()
        task_def = (filenames[0], file_contents, function)
        result = asyncio.run(self.distributeTasksets(pack(task_def), args))
        return result