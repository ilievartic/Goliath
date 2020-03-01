# goliath

### Abstract
This package enables python coders to build "multi-threaded" programs and optimize their data processing.
authors: Mani, Logan, Deepan, Ilie, Zachary

### Details

Oftentimes, python coders will need to handle large amounts of data or tasks. Ideally, they would be able to utilize the thread-based model when the data processing could be separated into independent chunks.
However, python's support for concurrency is essentially fake. Python's substitute for the thread model is a turn-based system where different "threads" take turns running at a time. This can be frustrating for programmers trying to implement actual thread-based programs.

goliath is a python package which enables programmers to distribute operations over a variable number of servers, which are in turn specified by the coder. This essentially simulates the "thread-based model", but instead replaces each thread with an independent process on a server.
goliath abstracts the communication with the servers and aggregates the results from each server's processes, finally returning the processed data to the coder.

### Installation:
Run:
`pip install goliath`

Then, in python script:
`from goliath import commander`

The commander module contains the interface with which the programmer must interact. 

Prerequisites:
+ Python3

### Licensing

goliath is open-source software, licensed under GNU's Lesser GPL.
