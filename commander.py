import asyncio
import time
from utils import *
class Commander:
    def __init__(self):
        self.loop = asyncio.get_event_loop()
    
    async def __wait_host_port(self, host, port, duration=10, delay=2):
        """
        Repeatedly try if a port on a host is open until duration seconds passed
        Parameters
        ----------
        host : str
            host ip address or hostname
        port : int
            port number
        duration : int, optional
            Total duration in seconds to wait, by default 10
        delay : int, optional
            delay in seconds between each try, by default 2
        
        Returns
        -------
        awaitable bool
        """
        if (host == ""):
            return False
        tmax = time.time() + duration
        while time.time() < tmax:
            try:
                _reader, writer = await asyncio.open_connection(host, port)
                writer.close()
                if hasattr(writer, 'wait_closed'):
                    await writer.wait_closed()
                return True
            except ConnectionRefusedError:
                if delay:
                    await asyncio.sleep(delay)
                    continue;
        return False
    async def __setHostsAndPorts(self, listHost, listPort):
        """
        Parameters
        ----------
        listHost: list of host strings
        listPort: list of ports (integer);
        """
        
        if (len(listHost) != len(listPort)):
            raise ValueError('listHost size does not equal listPort size')
        if (len(listHost) == 0):
            raise ValueError('listHost is empty');
        for idx in range(len(listHost)):
            tempResult = await self.__wait_host_port(listHost[idx], listPort[idx] ,10, 2)
            if (tempResult == False):
                raise ValueError('Unable to establish connection')
        self.listHost = listHost
        self.listPort = listPort
        
    def setHostsAndPorts(self, listHost, listPort):
         return self.loop.run_until_complete(self.__setHostsAndPorts(listHost, listPort))

    def __openAllConnections(self):
        """
        Open connections to all the user specified servers
        """
        
        listConnections = []
        for idx in range(len(self.listHost)):
            host = self.listHost[idx]
            port = self.listPort[idx];
            reader, writer = await asyncio.open_connection(host, port);
            listConnections.append((reader,writer));
        return listConnections;
    
    async def __pollLieutenantStatus(self, listConnections):
        """
        Poll each of the lieutenants to determine their status, num workers,and tasks to do
        Parameters
        ---------
        listConnections: List of Reader, writer connection tuples
        """
        
        listStatusData = []
        for idx in range(len(self.listHost)):
            reader = listConnections[idx][0]
            writer = listConnections[idx][1]
            listStringStatusPoll = [STATUS_TOKEN, "?"]
            stringStatusPoll = " ".join(listStringStatusPoll) + "\n"
            writer.write(stringStatusPoll)
            await writer.drain();
            statusResponse = await reader.readline().strip().split(" ")
            if (statusResponse[-1] == "."):
                if (statusResponse[0] == STATUS_TOKEN):
                    numWorkerParam, numWorkers = parseParameter(statusResponse[1])
                    queueSizeParam, queueSize = parseParameter(statusResponse[2])
                    listStatusData.append((int(numWorkers), int(queueSize)));            

        return listStatusData;

    async def __callLieutenant(self, taskDefPacked, tidArgumentsPacked, reader, writer):
        """
        Call the Lieutenant with the given task set
        Parameters
        ----------
        taskDefPacked: generic task data
        tidArgumentsPacked: specific task arguments that have been packed
        reader: reader created in openConnections
        writer: writer created in openConnections
        """
        listStringTask = [TASKSET_TOKEN, buildParameter(TASKDEF_PARAM, taskDefPacked), buildParameter(TASKLIST_PARAM,tidArgumentsPacked), "?"]
                
        toLieutenantMessage = " ".join(listStringBase) + "\n"
        writer.write(toLieutenantMessage);
        await writer.drain()
        
        #writer.close()
        #if hasattr(writer, 'wait_closed'):
         #   await writer.wait_closed()

    async def __readFromLieutenant(self, reader, writer):
        dataResponse = await reader.readline().strip().split(" ")
        if (dataResponse[-1] != "."):
            return []
        if (dataResponse[0] != TASKSET_TOKEN):
            return []
        dataOutputFromTask = dataResponse[1]
        
        writer.close()
        if hasattr(writer, 'wait_closed'):
            await writer.wait_closed()
        return dataOutputFromTask

    async def __sendTasksToLieutenant(self, taskDefPacked, tidArgumentsPairs):
        """
        Given the packed generic task definition, compute the distribution of tasks among lieutenants and send those tasks
        Finally, read the return data from each task
        Parameters
        ----------
        taskDefPacked: generic task data
        tidArgumentsPairs: all unpacked task specific arguments
        """
        listConnections = await self.__openAllConnections()
        listStatusData = await self.__pollLieutenantStatus(listConnections)
        totalNumWorkers = sum(listStatusData[0])
        totalNumTasks = len(tidArgumentsPairs)
        listNumTasksPerLieutenant = []
        numberOfTasksAssigned = 0
        appendZero = False
        for idx in range(len(listConnections)):
            if (appendZero):
                listNumTasksPerLieutenant.append(0)
                continue
            proportionOfTotalTasks = int((listStatusData[idx][0] / totalNumWorkers) * totalNumTasks)
            if (numberOfTasksAssigned + proportionOfTotalTasks > totalNumTasks):
                listNumTasksPerLieutenant.append(totalNumTasks - numberOfTasksAssigned)
                appendZero = True
                continue
            numberOfTasksAssigned += proportionOfTotalTasks;
            listNumTasksPerLieutenant.append(proportionOfTotalTasks);
            if (idx + 1 == len(listConnections) - 1):
                listNumTasksPerLieutenant.append(max(0, totalNumTasks - numberOfTasksAssigned))
                break
        prevIdx = 0 #keep track of initial idx in array slice of tidArgumentsPairs
        idxOfStop = len(listNumTasksPerLieutenant)
        for idx in range(len(listNumTasksPerLieutenant)):
            await self.__callLieutenant(taskDefPacked, pack(tidArgumentsPairs[prevIdx:prevIdx + listNumTasksPerLieutenant[idx]])
                                        , listConnections[idx][0], listConnections[idx][1])
            prevIdx = prevIdx + listNumTasksPerLieutenant[idx];
            if (prevIdx == len(listNumTasksPerLieutenant)):
                idxOfStop = idx + 1
                break;

        data_out = [];
        for i in range(idxOfStop):
            data_out += await self.__readFromLieutenant(listConnections[idx][0], listConnections[idx][1])
        data_out.sort() #this will sort the list of tuples by the first tuple element, which in this case is the Task ID

        return list(zip(*data_out))[:, 1]
        

    def runCommander(taskDef, dictionaryArgs):
        """
        Parameters
        ----------
        taskDef: the general task definition that all tasks will need to reference. See utils.py for class definition
        dictionaryArgs: a list of dictionaries. Each dictionary is associated with the parameters of a different task. Each K-V entry in the dictionary
                        represents the Name of the variable - Value
                        example: [{a:2, b:3}, {a:5, b:3}]
        """
        tidArgumentsPairs = zip(range(len(dictionaryArgs)), dictionaryArgs)

        
        taskDefPacked = pack(taskDef)
       
        return self.loop.run_until_complete(self.__sendTasksToLieutenant(taskDefPacked, tidArgumentPairs))
        
        
    
x = Commander()
list1 = ["0.0.0.0"];
list2 = [135];
x.setHostsAndPorts(list1, list2)
