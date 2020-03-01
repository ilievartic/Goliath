import commander

def foo(a):
    return 1 + 2 + 3 + a

if __name__ == '__main__':
    c = commander.Commander([('127.0.0.1', 31337)])
    test = c.run('foo', [{'a': 4}, {'a': 1}, {'a': 8}], ['tester.py'])
    print(test)
