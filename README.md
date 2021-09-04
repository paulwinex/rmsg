# Simple Redis Messaging

Simple message queue based on the Redis and PyRSMQ. 

(This is just test prototype. Don't use it in you projects!)

### Example

First host

```python
from rmsg import Sender, Receiver
# create sender
s = Sender(channel_name='example')
# now send messages any time
s.send('Hello')
s.send(dict(key='value'))
```

Second host

```python
from rmsg import Receiver

r = Receiver(channel_name='example', callback=lambda x: print(x))
# start listening new messages with blocking operation
r.watch()
```

You can create any count of senders with same queue name. Each of them will be sending messages to one queue.

You can create any count of receivers, but one message can receive only one receiver. 
Therefore, receivers will process messages one after another in turn like distributed workers.
