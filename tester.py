import commander

def foo(a):
    return 1 + 2 + 3 + a

def bar(b):
    return 100 - b

if __name__ == '__main__':
    c = commander.Commander([('localhost', 31337)])
    test = c.run('foo', [{'a': 4}, {'a': 1}, {'a': 8}], ['tester.py'])
    print(test)
    test2 = c.run('foo', [{'a': 4}, {'a': 1}, {'a': 8}], ['tester.py'])
    print(test2)
    test3 = c.run('bar', [{'b': 9}, {'b': 1000}], ['tester.py'])
    print(test3)
