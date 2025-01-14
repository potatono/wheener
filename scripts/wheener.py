#!/usr/bin/python3

import time
import threading
import json
import os
import sys
import re
import configparser
import argparse
import logging
import subprocess
import shutil
import signal
import http.server

import requests
import firebase_admin
from firebase_admin import credentials, firestore
from firebase_admin.firestore import FieldFilter

class Wheener:
    def __init__(self):
        self.rq = []
        self.viewers = 0

        self.init_args()
        self.init_config()
        self.init_logging()
        self.run_rpc_requests()
        self.init_signal()
        self.init_firebase()
        self.init_webserver()
        self.review()

    def init_args(self):
        parser = argparse.ArgumentParser()

        self.root_path = os.path.realpath(os.path.join(os.path.dirname(__file__),".."))
        
        config=os.path.join(self.root_path, "conf", "wheener.conf")
        secrets=os.path.join(self.root_path, "conf", "wheener-secrets.conf")
        stream=os.getenv('MTX_PATH')

        parser.add_argument("-f", "--config", default=config)
        parser.add_argument("-S", "--secrets", default=secrets)
        parser.add_argument("-s", "--stream", default=stream)
        parser.add_argument("-c", "--channel", default=None)
        parser.add_argument("-v", "--verbose", action='store_true')
        parser.add_argument("-A", "--add-viewer", action='store_true')
        parser.add_argument("-R", "--remove-viewer", action='store_true')

        self.args = parser.parse_args()

        if self.args.stream is None:
            print("No stream specified in MTX_PATH or with -s.  Aborting.")
            sys.exit(1)

        if not os.path.exists(self.args.config):
            print(f"Config path {self.args.config} does not exist")
            sys.exit(1)

        if not os.path.exists(self.args.secrets):
            print(f"Secrets path {self.args.secrets} does not exist")
            sys.exit(1)

    def init_config(self):
        self.config = configparser.ConfigParser()
        self.config.read(self.args.config)

        self.secrets = configparser.ConfigParser()
        self.secrets.read(self.args.secrets)

        self.rtmp_forward = False
        self.channel = self.args.channel
        self.whep_url = f"http://localhost:8889/{self.args.stream}/whep"
        self.rtsp_url = f"rtsp://localhost:8554/{self.args.stream}"
        self.rtmp_url = None
        self.max_connections = 5

        stream_section = f"stream.{self.args.stream}"
        
        if self.config.has_section(stream_section):
            self.channel = self.channel or self.config.get(stream_section, "stooth_channel")
            self.whep_url = self.config.get(stream_section, "whep_url", fallback=self.whep_url)
            self.rtsp_url = self.config.get(stream_section, "rtsp_url", fallback=self.rtsp_url)
            self.rtmp_forward = self.config.getboolean(stream_section, "rtmp_forwarding", fallback=False)
            self.rtmp_url = self.config.get(stream_section, "rtmp_url", fallback=None)
            self.max_connections = self.config.getint(stream_section, "max_connections", fallback=self.max_connections)

            if self.secrets.has_section(stream_section):
                self.rtmp_stream_key = self.secrets.get(stream_section, "rtmp_stream_key", fallback=None)

    def init_logging(self):
        self.log = logging.getLogger(__name__)
        level = self.config.get('DEFAULT', 'log_level', fallback='INFO')

        # Override log level with -v
        if self.args.verbose:
            level = 'DEBUG'

        format = self.config.get('DEFAULT', 'log_format', fallback='[%(asctime)s %(levelname)s] %(message)s')

        logging.basicConfig(level=logging.getLevelName(level), format=format)

        path = self.config.get('DEFAULT', 'log_path', fallback=None)
        if path:
            self.log.propagate = True
            self.log_fh = logging.FileHandler(path)
            self.log.addHandler(self.log_fh)

    def init_firebase(self):
        key_path = os.path.join(self.root_path, self.config.get('DEFAULT', 'firebase_key_path'))
        cred = credentials.Certificate(key_path)
        firebase_admin.initialize_app(cred)
        self.db = firestore.client()

    def init_webserver(self):
        address = self.config.get("webserver", "address", fallback="0.0.0.0")
        port = self.config.getint("webserver", "port", fallback=9436)
        port = self.config.getint(f"stream.{self.args.stream}", "webserver_port", fallback=port)

        class RequestHandler(http.server.BaseHTTPRequestHandler):
            def respond(this, response, type, code=200):
                this.send_response(code)
                this.send_header('Content-type', type)
                this.end_headers()
                this.wfile.write(response.encode())
                this.wfile.write("\n".encode())

            def do(this, method):
                try:
                    response = self.handle_webserver_request(method, this.path)
                    if response:
                        if type(response) is dict:
                            this.respond(json.dumps(response), 'application/json')
                        else:
                            this.respond(response, 'text/html')
                    else:
                        this.respond("Not Found", "text/plain", 404)

                except Exception as ex:
                    self.log.error(ex)
                    this.respond(str(ex), "text/plain", 500)

            def do_GET(this):
                this.do("GET")

            def do_POST(this):
                this.do("POST")

        self.webserver = http.server.ThreadingHTTPServer((address, port), RequestHandler)
        self.webthread = threading.Thread(target=self.run_webserver)
        self.webthread.start()

    def run_webserver(self):
        self.log.info(f"Starting webserver on {self.webserver.server_address}...")
        self.webserver.serve_forever()

    def handle_webserver_request(self, method, path):
        if method == "POST" and path == f"/{self.args.stream}/viewers/add":
            return self.update_viewers(1)
        elif method == "POST" and path == f"/{self.args.stream}/viewers/remove":
            return self.update_viewers(-1)

        return None

    def update_viewers(self, delta=1):
        self.viewers += delta
        self.log.info(f"Viewer count has changed by {delta} to {self.viewers}.")
        return {"viewers": self.viewers}

    def request_update_viewers(self, change="add"):
        port = self.config.getint("webserver", "port", fallback=9436)
        port = self.config.getint(f"stream.{self.args.stream}", "webserver_port", fallback=port)

        url = f"http://localhost:{port}/{self.args.stream}/viewers/{change}"
        self.log.info(f"Requesting viewer update at {url}.")

        res = requests.post(url)

        if res.status_code != 200:
            self.log.error(f"While requesting viewer update: {res}")

    def run_rpc_requests(self):
        if self.args.add_viewer:
            self.request_update_viewers("add")
            sys.exit(0)
        elif self.args.remove_viewer:
            self.request_update_viewers("remove")
            sys.exit(0)
        
    def shutdown(self, sig, frame):
        self.log.info("Shutting down..")

        self.webserver.shutdown()
        self.webthread.join()

        if hasattr(self, 'fwd_thread'):
            self.fwd_thread.join()

        sys.exit(0)

    def init_signal(self):        
        signal.signal(signal.SIGINT, self.shutdown)

    def review(self):
        self.log.info("Wheener Starting...")
        self.log.info(f"stream tooth channel={self.channel}")
        self.log.info(f"whep url={self.whep_url}")
        self.log.info(f"rtmp_forwarding={self.rtmp_forward}")
        self.log.info(f"rtmp_url={self.rtmp_url}")

    def get_channel_colref(self):
        return (self.db.collection("channels").document(self.channel)
                .collection("peers"))

    def create_message_id(self):
        return int(time.time() * 1000.0)

    def send_answer(self, answer, offer):
        self.log.info(f"Appending answer to {self.channel} collection...")

        text = json.dumps({"type":"answer", "sdp": answer})

        data = {
            "_0_id": self.create_message_id(),
            "_1_replyTo": offer['id'],
            "_2_timestamp": firestore.SERVER_TIMESTAMP,
            "_3_from": "origin",
            "_4_to": offer['from'],
            "_5_type": "answer",
            "_6_text": text
        }

        colref = self.get_channel_colref()
        _, doc = colref.add(data)

        return doc.id

    # def update_offer(self, offer_id, answer_id):
    #     self.log.info("Updating offer with answerId..")
    #     doc = self.get_channel_colref().document(offer_id)
    #     doc.update({ 
    #         "timestamp": firestore.SERVER_TIMESTAMP,
    #         "answerId": answer_id 
    #     })

    def forward_offer(self, offer):
        self.log.info("POSTing offer to MediaMTX...")

        headers = { "Content-Type": "application/sdp" }
        res = requests.post(self.whep_url, data=offer, headers=headers)
        
        if res.status_code == 201:
            return str(res.content, encoding="utf-8")
        else:
            self.log.error(f"Unexpected response: {res.status_code}")
            self.log.error(res.content)
            return None        

    def on_offer_available(self, offer_id, offer):
        if self.viewers >= self.max_connections:
            self.log.info(f"Refusing to send answer, viewer count {self.viewers} is at max.")
            return

        # Get the SDP text from the offer document
        sdp = json.loads(offer['text'])['sdp']

        # Forward the offer to MediaMTX
        answer = self.forward_offer(sdp)

        # If we got an answer, send it back to firebase
        # and update the offer with the id of the answer for later
        if answer:
            self.send_answer(answer, offer)
            #self.update_offer(offer_id, answer_id)

    def on_logs(self, doc_id, data):
        payload = json.loads(data['text'])

        path = self.config.get('DEFAULT', 'client_log_path', fallback=None)
        if path:
            with open(path, "a") as fil:

                fil.write(f"Logs from {data['from']} since {payload['startTime']}:\n")
                fil.write(payload['text'])
                fil.write("\n")

        self.log.debug(f"\n--- Client {data['from']} ---\n{payload['text']}\n--- /Client {data['from']} ---")

    def from_firestore(self, message):
        return {
            'id': message['_0_id'],
            'reply_to': message['_1_replyTo'],
            'timestamp': message['_2_timestamp'],
            'from': message['_3_from'],
            'to': message['_4_to'],
            'type': message['_5_type'],
            'text': message['_6_text']
        }
    
    def listen(self):
        self.log.info("Init listener..")
        callback_done = threading.Event()

        def callback(snap, changes, read_time):
            for change in changes:
                if change.type.name == "ADDED":
                    doc = change.document
                    data = self.from_firestore(doc.to_dict())

                    if data['type'] == "offer":
                        self.on_offer_available(doc.id, data)

                    if data['type'] == "logs":
                        self.on_logs(doc.id, data)

                    self.rq.append({
                        "id": doc.id,
                        "received": time.time()
                    })

            callback_done.set()

        ref = self.get_channel_colref()
        ref.on_snapshot(callback)

    def clear(self):
        self.log.info("Clearing collection..")
        colref = self.get_channel_colref()
        docs = colref.list_documents()
        for doc in docs:
            doc.delete()

    def get_forwarding_cmd(self):
        ffmpeg = shutil.which('ffmpeg')
        
        if ffmpeg is None:
            self.log.error("Cannot forward stream, ffmpeg not found in PATH.  Aborting.")
            sys.exit(1)

        rtmp_url = self.rtmp_url

        if rtmp_url is None:
            self.log.error("Cannot forward stream, no RTMP url given.  Aborting.")
            sys.exit(1)

        if self.rtmp_stream_key is not None:
            rtmp_url += "/" + self.rtmp_stream_key

        cmd_args = [
            ffmpeg,
            "-i", self.rtsp_url,
            "-threads","4",

            # Container
            "-f","flv",

            # Video
            "-c:v","copy",
            #"-pix_fmt","yuvj420p", 
            #"-x264-params","keyint=48:min-keyint=48:scenecut=-1",
            #"-b:v","6000k",
            #"-vcodec","libx264",
            #"-preset","medium",
            #"-crf","28",

            # Audio
            "-b:a","128k",
            "-ar","44100",
            "-acodec","aac",
            
            rtmp_url
        ]

        return cmd_args
    
    def run_forwarding(self, cmd):
        with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as proc:
            while proc.poll() is None:
                line = str(proc.stdout.readline(), encoding="utf-8").rstrip("\n")

                # Prevent accidental reveal of stream key
                if hasattr(self, 'rtmp_stream_key'):
                    line = re.sub(self.rtmp_stream_key, "[STREAM_KEY]", line)

                self.log.debug(f"ffmpeg: {line}")

    def start_forwarding(self):
        if self.rtmp_forward:
            cmd = self.get_forwarding_cmd()
            self.fwd_thread = threading.Thread(target=self.run_forwarding, args=(cmd,))
            self.fwd_thread.start()

    def clean_queue(self):
        dead_time = time.time() - 60
        while len(self.rq) > 0 and self.rq[0]['received'] <= dead_time:
            dead = self.rq.pop(0)
            self.log.debug(f"Deleting old message {dead['id']}")
            doc = self.get_channel_colref().document(dead['id'])
            doc.delete()
            
    def run(self):
        self.start_forwarding()
        self.clear()
        self.listen()

        while True:
            self.log.debug(f"Stream {self.args.stream} is live with {self.viewers} viewers.")
            self.clean_queue()
            time.sleep(10)

if __name__ == "__main__":
    wh = Wheener()
    wh.run()


