import commander

def foo(a):
    return 1 + 2 + 3 + a

def bar(b):
    return 100 - b

if __name__ == '__main__':
    c = commander.Commander([('localhost', 31337)])
    test = c.run('testFunc', [{'a': 7}], ['testsource.py'])
    print(test)
