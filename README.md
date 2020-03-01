# goliath

### Authors
Manikandan Swaminathan, Logan Pulley, Deepan Venkatesh, Ilie Vartic, Zachary Oldham

### Abstract
This package enables python coders to build "multi-threaded" programs and optimize their data processing.

### Details

Oftentimes, python coders will need to handle large amounts of data or tasks. Ideally, they would be able to utilize the thread-based model when the data processing could be separated into independent chunks.
However, python's support for concurrency is essentially fake. Python's substitute for the thread model is a turn-based system where different "threads" take turns running at a time. This can be frustrating for programmers trying to implement actual thread-based programs.

goliath is a python package which enables programmers to distribute operations over a variable number of remote servers, which are in turn specified by the coder. This essentially simulates the "thread-based model", but instead replaces each thread with an independent process on a server. By using remote servers, goliath enables a many-to-many relationship where multiple clients can have associated processes on the same server, while one client can also have processes distributed across multiple servers.

goliath abstracts the communication with the servers and aggregates the results from each server's processes, finally returning the processed data to the coder.

### Installation:
Run:
`pip install goliath`

Then, in python script:
`from goliath import commander`

The commander module contains the interface with which the programmer must interact. 

Prerequisites:
+ Python3.8

### Licensing

goliath is open-source software, licensed under GNU's Lesser GPL.
