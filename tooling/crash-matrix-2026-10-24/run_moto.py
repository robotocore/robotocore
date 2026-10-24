"""Start the uv-cached moto fork (5.1.23.dev) as a scratch server on :4795."""
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

srv = ThreadedMotoServer("127.0.0.1", 4795)
srv.start()
print("moto up on 4795", flush=True)
import signal

signal.pause()
