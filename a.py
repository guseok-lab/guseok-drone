# -*- coding: utf-8 -*-
import http.server, threading, webbrowser

html = b"""<!DOCTYPE html>
<html>
<body style="background:#111;display:flex;flex-direction:column;align-items:center;gap:12px;padding:20px">
  <h2 style="color:#fff">Stream Test</h2>
  <img src="http://localhost:5001/video/drone-c96ebe1c"
       style="width:640px;border-radius:8px">
  <p style="color:#aaa">live stream - should be moving</p>
</body>
</html>"""

class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(html)
    def log_message(self, *a):
        pass

s = http.server.HTTPServer(('', 8888), H)
threading.Thread(target=s.serve_forever, daemon=True).start()
webbrowser.open('http://localhost:8888')
input('press enter to quit\n')
