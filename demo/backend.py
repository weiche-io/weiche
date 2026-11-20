"""HTTP/1.1 echo + chunked SSE backend using only the Python standard library."""
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if self.path.split("?", 1)[0].endswith("/events"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            try:
                for index in range(3):
                    chunk = f"data: {index}\n\n".encode()
                    self.wfile.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
                    self.wfile.flush()
                    time.sleep(0.5)
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            return
        self.echo(b"")

    def do_POST(self):
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            chunks = []
            while True:
                size = int(self.rfile.readline().strip().split(b";", 1)[0], 16)
                if size == 0:
                    while self.rfile.readline() != b"\r\n":
                        pass
                    break
                chunks.append(self.rfile.read(size))
                assert self.rfile.read(2) == b"\r\n"
            body = b"".join(chunks)
        else:
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        self.echo(body)

    def echo(self, body):
        payload = json.dumps({"backend": os.environ.get("BACKEND_NAME", "echo"), "pod": os.environ.get("HOSTNAME"),
                              "method": self.command, "path": self.path, "host": self.headers.get("Host"),
                              "body": body.decode(), "headers": dict(self.headers)}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Connection", "keep-alive, x-backend-hop")
        self.send_header("X-Backend-Hop", "must-be-stripped")
        self.end_headers()
        self.wfile.write(payload)


ThreadingHTTPServer(("0.0.0.0", 9000), Handler).serve_forever()
