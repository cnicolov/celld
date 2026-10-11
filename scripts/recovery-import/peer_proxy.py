"""TCP peer proxy throttling only recovery-tail responses, including keepalive."""

import socket
import socketserver
import threading
import time


class PeerProxy(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, listen_port, target_port, kib_per_second):
        self.target_port = target_port
        self.rate = kib_per_second * 1024
        self.tail_connections = 0
        self.tail_wire_bytes = 0
        self.lock = threading.Lock()
        super().__init__(("127.0.0.1", listen_port), Handler)
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.shutdown()
        self.server_close()
        self.thread.join()


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            header = b""
            while b"\r\n\r\n" not in header:
                block = self.request.recv(65536)
                if not block:
                    return
                header += block
                if len(header) > 256 * 1024:
                    raise ValueError("oversized peer request header")
            tail = header.startswith(b"POST /peer/log/tail-page ")
            throttled = tail and self.server.rate > 0
            if tail:
                with self.server.lock:
                    self.server.tail_connections += 1
            with socket.create_connection(("127.0.0.1", self.server.target_port), timeout=10) as peer:
                peer.settimeout(None)
                peer.sendall(header)

                def upload():
                    try:
                        while block := self.request.recv(65536):
                            peer.sendall(block)
                    except OSError:
                        pass
                    finally:
                        try:
                            peer.shutdown(socket.SHUT_WR)
                        except OSError:
                            pass

                uploader = threading.Thread(target=upload, daemon=True)
                uploader.start()
                while block := peer.recv(65536):
                    self.request.sendall(block)
                    if tail:
                        with self.server.lock:
                            self.server.tail_wire_bytes += len(block)
                    if throttled:
                        time.sleep(len(block) / self.server.rate)
                try:
                    self.request.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                uploader.join(timeout=1)
        except (OSError, ValueError):
            pass
