import asyncio
import time
from utils import *
class Commander:
    def __init__(self):
        self.loop = asyncio.get_event_loop()
    
    async def __wait_host_port(self, host, port, duration=10, delay=2):
        """Repeatedly try if a port on a host is open until duration seconds passed
        
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

    async def __callLieutenant(taskDefPacked, tidArgumentsPacked):
        reader, writer = await asyncio.open_connection(host, port)
        listStringBase = [TASKSET_TOKEN, " ", TASKDEF_PARAM, ":", taskDefPacked, " ", TASKLIST_PARAM, ":", tidArgumentsPacked, " ?"]
        
        toLieutenantMessage = ''.join(listStringBase)
        writer.write(toLieutenantMessage);
        await writer.drain()
        
        writer.close()
        if hasattr(writer, 'wait_closed'):
            await writer.wait_closed()

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
        taskDefPacked = pack(taskDef);
        tidArgumentsPacked = pack(tidArgumentPairs);
        #For now, just pass to first host/port. Optimize later
        #self.loop.run_until_complete(self.__callLieutenant(taskDefPacked, tidArgumentsPacked))
        
        
    
x = Commander()
list1 = ["0.0.0.0"];
list2 = [135];
x.setHostsAndPorts(list1, list2)
