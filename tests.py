import asyncio
import hashlib
import numpy as np
import pytest

from setup import Setup


PEERSTER_ROOT = "/home/thomas/go/src/github.com/thomashlvt/Peerster"


@pytest.mark.skip(reason="Use wrappers around these async methods")
class Tests:
    @staticmethod
    async def test_private_messages():
        NUM_PEERS = 50

        conn_matrix = np.zeros((NUM_PEERS, NUM_PEERS))
        np.fill_diagonal(conn_matrix[:, 1:], 1)
        np.fill_diagonal(conn_matrix[1:, :], 1)

        s = Setup(PEERSTER_ROOT, NUM_PEERS, conn_matrix, anti_entropy=1)

        s.run_all()
        await asyncio.sleep(1)

        for peerster in s.peersters:
            peerster.send_public_message(f" ")
        await asyncio.sleep(10)

        msg_map = {}
        for i, peerster in enumerate(s.peersters):
            if i - 7 < 0 and not i + 7 >= len(s.peersters):
                other = i + 7
            else:
                other = i - 7

            msg_map[other] = msg_map.get(other, []) + [i]

            peerster.send_private_message(f"Test{i}",
                                          f"testPeer{other}")
        await asyncio.sleep(30)

        await s.stop_all()

        for i, peerster in enumerate(s.peersters):
            others = msg_map.get(i)
            if others is None:
                continue
            for other in others:
                peer = f"testPeer{other}"
                assert peer in peerster.private_messages.keys()
                assert len(peerster.private_messages[peer]) == 1
                assert peerster.private_messages[peer][0] == {"msg": f"Test{other}", "hop-lim": "3"}

    @staticmethod
    async def test_public_messages():
        NUM_PEERS = 50

        conn_matrix = np.zeros((NUM_PEERS, NUM_PEERS))
        np.fill_diagonal(conn_matrix[:, 1:], 1)
        np.fill_diagonal(conn_matrix[1:, :], 1)

        print(conn_matrix)

        s = Setup(PEERSTER_ROOT, NUM_PEERS, conn_matrix, anti_entropy=2)

        s.run_all()
        await asyncio.sleep(1)

        for i, peerster in enumerate(s.peersters):
            peerster.send_public_message(f"Test{i}")
        await asyncio.sleep(10)

        await s.stop_all()

        for i, peerster in enumerate(s.peersters):
            print(peerster.public_messages)
            for j, other_peerster in enumerate(s.peersters):
                if i == j:
                    continue
                assert peerster.public_messages[f"testPeer{j}"] == {1: f"Test{j}"}

    @staticmethod
    async def test_file_upload():
        NUM_PEERS = 10

        conn_matrix = np.zeros((NUM_PEERS, NUM_PEERS))
        np.fill_diagonal(conn_matrix[:, 1:], 1)
        np.fill_diagonal(conn_matrix[1:, :], 1)

        s = Setup(PEERSTER_ROOT, NUM_PEERS, conn_matrix, anti_entropy=2)

        s.run_all()
        await asyncio.sleep(.5)

        # Upload files
        files = []
        for i, peerster in enumerate(s.peersters):
            with open(f"{peerster.root}/_SharedFiles/test{i}.txt", "w+") as f:
                to_file = "12345678" * 1000 + peerster.name + "12345678" * 1000
                files.append(to_file)
                f.write(to_file)
            peerster.upload_file(f"test{i}.txt")
        await asyncio.sleep(.5)

        await s.stop_all()

        # Assert files are uploaded
        for i, peerster in enumerate(s.peersters):
            with open(f"{peerster.root}/_SharedFiles/test{i}.txt", "rb") as f:
                h = Tests._calc_hash(f)
            assert h in peerster.uploaded_files

    @staticmethod
    async def test_file_download():
        NUM_PEERS = 10

        conn_matrix = np.zeros((NUM_PEERS, NUM_PEERS))
        np.fill_diagonal(conn_matrix[:, 1:], 1)
        np.fill_diagonal(conn_matrix[1:, :], 1)

        s = Setup(PEERSTER_ROOT, NUM_PEERS, conn_matrix, anti_entropy=2)

        s.run_all()
        await asyncio.sleep(.5)

        # Send route rumors
        for i, peerster in enumerate(s.peersters):
            peerster.send_public_message(f" ")

        # Upload files
        files = []
        for i, peerster in enumerate(s.peersters):
            with open(f"{peerster.root}/_SharedFiles/test{i}.txt", "w+") as f:
                to_file = "12345678" * 1000 + peerster.name + "12345678" * 1000
                files.append(to_file)
                f.write(to_file)
            peerster.upload_file(f"test{i}.txt")
        await asyncio.sleep(.5)

        # Don't calculate hash ourselves, but use peerster hashes: test doesn't fail
        # if hashing/chunking is done incorrectly
        metas = []
        for i, peerster in enumerate(s.peersters):
            assert len(peerster.uploaded_files) == 1
            hash = next(iter(peerster.uploaded_files))
            assert len(hash) == 64
            metas.append(hash)

        await asyncio.sleep(2)

        for i, peerster in enumerate(s.peersters):
            other = (i - 7) % len(s.peersters)
            peerster.download_file(f"test{other}_d.txt", metas[other], f"testPeer{other}")

        await asyncio.sleep(10)
        for i, peerster in enumerate(s.peersters):
            assert f"test{(i-7)%len(s.peersters)}_d.txt" in peerster.downloaded_files

        await s.stop_all()

    @staticmethod
    def _calc_hash(f):
        chunk_size = 8192
        hs = b""
        while True:
            bs = f.read(chunk_size)
            if len(bs) == 0:
                break
            hs += hashlib.sha256(bs).digest()
        return hashlib.sha256(hs).digest().hex()


# -- PyTest Tests -- #

def test_private_messages():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(Tests.test_private_messages())


def test_public_messages():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(Tests.test_public_messages())


def test_file_upload():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(Tests.test_file_upload())


def test_file_download():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(Tests.test_file_download())
