"""
Simple example of bidirectional chat for two users
"""
import rmsg
import sys


if __name__ == '__main__':
    channel = sys.argv[-1]
    c = rmsg.Client(channel, lambda x: print('\r:', x, '\n> ', end=''))
    msg = ''
    try:
        while msg not in ('q', 'exit'):
            if msg:
                c.send(msg)
            msg = input('> ')
    except KeyboardInterrupt:
        print('\nI got to go...')
    c.send('Bye!')
    c.stop()
    sys.exit()
