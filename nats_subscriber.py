import asyncio
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

class NATSSubscriber:
    _subject: str
    _queue: bool
    _queue_name: str

    def __init__(self, subject, queue=False, queue_name="No Queue"):
        self.subject = subject
        self.queue = queue
        self.queue_name = queue_name

    @property
    def subject(self) -> str:
        return self._subject

    @subject.setter
    def subject(self, subject) -> None:
        self._subject = subject

    @property
    def queue_name(self) -> str:
        return self._queue_name

    @queue_name.setter
    def queue_name(self, queue_name) -> None:
        self._queue_name = queue_name

    @property
    def queue(self) -> str:
        return self._queue

    @queue.setter
    def queue(self, queue) -> None:
        self._queue = queue

    def start_subscriber(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run(loop))
        loop.close()

    async def run(self, loop):
        nc = NATS()

        async def disconnected_cb():
            print("Got disconnected...")

        async def reconnected_cb():
            print("Got reconnected...")

        try:
            await nc.connect(
                     reconnected_cb=reconnected_cb,
                     disconnected_cb=disconnected_cb,
                     max_reconnect_attempts=-1,
                     io_loop=loop),
        except ErrNoServers as e:
            print(e)
            return

        # Handling incoming messages
        async def message_handler(msg):
            data = msg.data.decode()
            # data = json.loads(msg.data.decode())
            print(data)
            # populate a locally defined object to pass around app.

        if self.queue:
            await nc.subscribe(self.subject, self.queue, cb=message_handler)
        else:
            await nc.subscribe(self.subject, cb=message_handler)
        print('Subscribed to [%s]' % self.subject);

        await nc.request()

        try:
            # Flush connection to server, returns when all messages have been processed.
            # It raises a timeout if roundtrip takes longer than 1 second.
            await nc.flush(1)
        except ErrTimeout:
            print("Flush timeout")
