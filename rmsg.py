from rsmq import RedisSMQ
from rsmq.cmd import QueueDoesNotExist
from rsmq.consumer import RedisSMQConsumer, RedisSMQConsumerThread
import redis
import threading
import queue
import sys
import time

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = '6379'


class Sender:
    def __init__(self, channel_name, redis_host=None, redis_port=None,
                 message_live_time=30, default_delay=0):
        self.queue = RedisSMQ(host=redis_host or DEFAULT_HOST,
                              port=redis_port or DEFAULT_PORT,
                              qname=channel_name
                              )
        try:
            self.queue.getQueueAttributes().execute()
        except QueueDoesNotExist:
            self.queue.exceptions(False).createQueue(delay=default_delay).vt(message_live_time).execute()

    def send(self, msg, delay=0):
        self.queue.sendMessage(delay=delay).message(msg).execute()

    def delete(self):
        self.queue.deleteQueue().exceptions(False).execute()

    def size(self):
        return self.queue.getQueueAttributes().execute()['msgs']


class Receiver:
    def __init__(self, channel_name, callback, redis_host=None, redis_port=None, threaded=False):
        self.queue = RedisSMQ(host=redis_host or 'localhost',
                              port=redis_port or '6379',
                              qname=channel_name
                              )
        self.callback = callback
        self.threaded = threaded
        if self.threaded:
            self.consumer = RedisSMQConsumerThread(channel_name, self._on_receive, host='localhost')
        else:
            self.consumer = RedisSMQConsumer(channel_name, self._on_receive, host='localhost', realtime=True)

    def watch(self):
        try:
            if self.threaded:
                self.consumer.start()
            else:
                self.consumer.run()
        except KeyboardInterrupt:
            pass

    def stop(self, delay=0):
        if not self.threaded:
            return False
        self.consumer.stop(delay)

    def _on_receive(self, **args):
        msg = Message(**args)
        self.callback(msg)
        return True


class Client:
    """Messaging for two clients only"""
    def __init__(self, channel_name, callback,
                 redis_host=None, redis_port=None):
        self.callback = callback
        self.channel_name = channel_name
        self._q = queue.Queue()
        self._lk = threading.Lock()
        self._t = None
        R = redis.Redis(host=redis_host or DEFAULT_HOST, port=redis_port or DEFAULT_PORT)
        c1 = channel_name+'-client-1'
        c2 = channel_name+'-client-2'
        v1, v2 = R.mget(c1, c2)
        if not v1:
            client_key = c1
            s, r = '-1', '-2'
        elif not v2:
            client_key = c2
            s, r = '-2', '-1'
        else:
            raise Exception('Channel "{}" already used'.format(channel_name))
        print('You are', client_key)
        self._lk.acquire()
        self._t = self._start_heartbeat(R, client_key)
        self.clear = lambda: R.delete(client_key)
        self.sender = Sender(redis_host=redis_host, redis_port=redis_port,
                             channel_name=channel_name+s)
        self.receiver = Receiver(redis_host=redis_host, redis_port=redis_port,
                                 channel_name=channel_name+r,
                                 callback=callback, threaded=True)
        self.receiver.watch()
        if client_key == c2:
            self.send('Hello from %s' % client_key)

    def _start_heartbeat(self, R, key):
        t = threading.Thread(target=self._pulse, args=(R, key), daemon=True)
        t.start()
        return t

    def _pulse(self, R, key):
        while True:
            if self._lk.locked():
                R.set(key, 1, ex=5)
                time.sleep(3)
            else:
                break

    def _stop_heartbeat(self):
        if self._lk.locked():
            self._lk.release()
        if self._t:
            while True:
                if self._t.is_alive():
                    time.sleep(1)
                    print('.', end='')
                else:
                    print()
                    break

    def send(self, msg, *args):
        self.sender.send(msg, *args)

    def stop(self):
        self._stop_heartbeat()
        if hasattr(self, 'receiver'):
            self.receiver.stop()
        if hasattr(self, 'clear'):
            self.clear()

    def __del__(self):
        self.stop()


class Message:
    def __init__(self, id, message, rc, ts):
        self.id = id
        self.data = message
        self.receive_count = rc
        self.timestamp = ts

    def __str__(self):
        return str(self.data)

    def __repr__(self):
        return repr(self.data)


if __name__ == '__main__':
    def on_message(msg):
        print(msg)
        time.sleep(0.5)
    if len(sys.argv) < 2:
        raise Exception('Set channel name')
    channel = sys.argv[-1]
    c = Receiver(callback=on_message, channel_name=channel)
    print('Start watching channel', channel)
    c.watch()
