import commander

def foo():
    return 1 + 2 + 3

if __name__ == '__main__':
    c = commander.Commander([('127.0.0.1', 31337)])
    test = c.run('foo', None, ['tester.py'])
    print(test)
