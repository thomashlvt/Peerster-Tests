import numpy as np
import os
import sys
import subprocess as sp
import re
from time import sleep
from datetime import datetime
from threading import Lock
import asyncio


class Peerster:

    def __init__(self, root, uiport, gossip_addr, name, peers, anti_entropy, rtimer):
        self.root = root
        self.uiport = uiport
        self.gossip_addr = gossip_addr
        self.name = name
        self.peers = peers

        self._start_cmd = [f"{root}/Peerster",
                           "-UIPort", uiport,
                           "-gossipAddr", gossip_addr,
                           "-name", name,
                           "-peers", ",".join(peers),
                           "-debug",
                           "-antiEntropy", str(anti_entropy),
                           "-rtimer", str(rtimer)]

        self._client_cmd = [os.path.join(root, "client/client"),
                            "-UIPort", self.uiport]
        self.mutex = Lock()

        self.peers = []
        self.dsdv = {}

        self.public_messages = {}
        self.public_messages_next_id = {}
        self.private_messages = {}
        self.uploaded_files = set()
        self.downloaded_files = set()
        self.client_messages = []
        self.in_sync_with = set()

        self.metafiles = []
        self.downloading = {}

        self.kill_sig = asyncio.Event()

    def _update_peers(self, peers):
        with self.mutex:
            self.peers = peers

    def _update_dsdv(self, peer, addr):
        with self.mutex:
            self.dsdv[peer] = addr

    def _update_public_messages(self, origin, id, content):
        with self.mutex:
            if self.public_messages.get(origin) is None:
                self.public_messages[origin] = {}
            self.public_messages[origin][int(id)] = content
            next_id = int(self.public_messages_next_id.get(origin, "1"))
            while self.public_messages[origin].get(next_id) is not None:
                next_id += 1
                self.public_messages_next_id[origin] = next_id

    def _update_in_sync_with(self, peer):
        with self.mutex:
            self.in_sync_with.add(peer)

    def _update_private_messages(self, msg, origin, hop_limit):
        with self.mutex:
            if self.private_messages.get(origin) is None:
                self.private_messages[origin] = []
            self.private_messages[origin].append({"msg": msg, "hop-lim": hop_limit})

    def _update_uploaded_files(self, meta):
        with self.mutex:
            self.uploaded_files.add(meta)

    def _update_downloaded_files(self, filename):
        with self.mutex:
            self.downloaded_files.add(filename)

    async def run(self):
        # start child process
        # NOTE: universal_newlines parameter is not supported
        self._killed = False
        process = await asyncio.create_subprocess_exec(*self._start_cmd,
                                                       stdout=asyncio.subprocess.PIPE,
                                                       cwd=self.root)

        # read line (sequence of bytes ending with b'\n') asynchronously
        with open(f"out/{self.name}.out", "w+") as f:
            while True:
                try:
                    line = await asyncio.wait_for(process.stdout.readline(), .1)
                except asyncio.TimeoutError:
                    if self.kill_sig.is_set():
                        process.kill()
                        return await process.wait()
                else:
                    line = line.decode(sys.stdout.encoding).strip()
                    if not line:  # EOF
                        break
                    else:
                        f.write(f"[{datetime.now()}] {line}\n")
                        if line[:len("PEERS")] == "PEERS":
                            self._update_peers(line[len("PEERS "):].split(","))
                        elif line[:len("DSDV")] == "DSDV":
                            origin, addr = line.split(" ")[1:]
                            self._update_dsdv(origin, addr)
                        elif line[:len("IN SYNC WITH")] == "IN SYNC WITH":
                            self._update_in_sync_with(line[len("IN SYNC WITH "):])
                        elif line[:len("RUMOR")] == "RUMOR":
                            m = re.match(r"RUMOR origin ([a-zA-Z0-9]+) from ([a-zA-Z0-9.:]+) ID ([0-9]+) contents ?(.*)$", line)
                            self._update_public_messages(m.groups()[0], m.groups()[2], m.groups()[3])
                        elif line[:len("PRIVATE")] == "PRIVATE":
                            m = re.match(r"PRIVATE origin ([a-zA-Z0-9]+) hop-limit ([0-9]+) contents ?(.*)$",
                                         line)
                            self._update_private_messages(m.groups()[2], m.groups()[0], m.groups()[1])
                        elif line[:len("CLIENT MESSAGE")] == "CLIENT MESSAGE":
                            line = line[len("CLIENT MESSAGE "):].strip()
                            if " dest " in line:
                                msg, dest = line.split(" dest ")
                                if dest == self.name:
                                    self._update_private_messages(msg, dest, hop_limit=10)
                            else:
                                self._update_public_messages(self.name,
                                                             len(self.public_messages.get(self.name, {}).keys()) + 1,
                                                             line)

                        elif line[:len("METAFILE")] == "METAFILE":
                            meta = line[len("METAFILE "):]
                            self._update_uploaded_files(meta)

                        elif line[:len("RECONSTRUCTED file")] == "RECONSTRUCTED file":
                            name = line[len("RECONSTRUCTED file "):]
                            self._update_downloaded_files(name)


            process.kill()
            return await process.wait()

    async def kill(self):
        self.kill_sig.set()

    def send_public_message(self, msg):
        proc = sp.run(self._client_cmd + ["-msg", msg], stdout=sp.PIPE)
        if proc.returncode != 0:
            raise RuntimeError(f"Could not send message '{msg}' with client {self.name}")

    def send_private_message(self, msg, to):
        proc = sp.run(self._client_cmd + ["-msg", msg] + ["-dest", to])
        if proc.returncode != 0:
            raise RuntimeError(f"Could not send private message '{msg}' with client {self.name}")

    def upload_file(self, file_name):
        proc = sp.run(self._client_cmd + ["-file", file_name])
        if proc.returncode != 0:
            raise RuntimeError(f"Could not upload file '{file_name}' with client {self.name}")

    def download_file(self, file_name, metadata, peer):
        proc = sp.run(self._client_cmd + ["-file", file_name, "-request", metadata, "-dest", peer])
        if proc.returncode != 0:
            raise RuntimeError(f"Could not download file '{file_name}' with client {self.name}")


class Setup:

    def __init__(self, root, num_peers, conn_matrix, anti_entropy=0, rtimer=0):
        # create connectivity matrix
        self.conn_matrix = conn_matrix

        # create peer lists
        peers = []
        for row in range(self.conn_matrix.shape[0]):
            p_ = []
            for col in range(self.conn_matrix.shape[1]):
                if self.conn_matrix[row][col]:
                    p_.append(f"127.0.0.1:{5000 + col}")
            peers.append(p_)

        # create peersters
        self.peersters = []
        for i in range(num_peers):
            self.peersters.append(Peerster(root,
                                           uiport=f"{8080+i}",
                                           gossip_addr=f"127.0.0.1:{5000+i}",
                                           name=f"testPeer{i}",
                                           peers=peers[i],
                                           anti_entropy=anti_entropy,
                                           rtimer=rtimer))

    def run_all(self):
        loop = asyncio.get_event_loop()
        self.tasks = []
        for peerster in self.peersters:
            self.tasks.append(loop.create_task(peerster.run()))

    async def stop_all(self):
        for peerster in self.peersters:
            await peerster.kill()
        await asyncio.gather(*self.tasks)
