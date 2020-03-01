from goliath.commander import Commander
from sys import argv

def foo(a):
    return a

if __name__ == '__main__':
    if len(argv) < 3:
        print('Please provide a hostname and port')
        exit(1)
    try:
        cmdr = Commander([(argv[1], int(argv[2]))])
        print('Result: {}'.format(cmdr.run(foo, [ {'a': 10} ] * 1, [ 'tb.py' ])))
    except ConnectionError as e:
        print('Connection failed!', file=sys.stderr)