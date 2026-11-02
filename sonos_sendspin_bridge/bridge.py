"""Sonos <- Sendspin bridge.

Makes a Sonos speaker a Sendspin player, so Music Assistant can play it in sync
with other Sendspin players (an Echo running pmaports-biscuit, for one).

A Sonos cannot be told *when* to play a sample - it plays an HTTP stream as soon
as it has buffered enough of it. What it does report, precisely (about 3 ms of
jitter on a LAN), is how far into that stream it is: playback:1 getPlaybackStatus
positionMillis. So the bridge turns the problem around. It serves the Sonos its
own live stream and, for every block it is about to write, works out from the
measured position when the Sonos will play that block, and fills it with the
Sendspin audio scheduled for exactly that moment. Clock drift between the Sonos
and this host is corrected one sample at a time; only a re-buffer or a skip
moves the stream by more.

A Sonos only starts once it holds about 6.8 s of a stream (2.3 s after
loadStreamUrl at best), so it cannot be restarted for every pause or skip.
The bridge keeps its stream running instead: a cold start primes it with 7 s of
near-silence (the Sonos joins the music some seconds late, then in sync), and
from then on its queue is held at about queue_ms, shorter than the Sendspin
lead, so new audio can always be placed exactly. On a stream end or clear the
queued audio would play on, so the Sonos is muted while that drains. The stream
stays up for idle_keep_s after the last stream, so the next play starts in sync.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import itertools
import json
import logging
import math
import os
import re
import socket
import ssl
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path

import aiohttp
import numpy as np
from aiohttp import web
from aiosendspin.client import SendspinClient
from aiosendspin.clock import RawMonotonicClock
from aiosendspin.models.core import DeviceInfo
from aiosendspin.models.player import ClientHelloPlayerSupport, SupportedAudioFormat
from aiosendspin.models.types import AudioCodec, PlayerCommand, Roles
from aiosendspin.noise.keys import Identity, b64url_decode
from aiosendspin.noise.trust_store import FileClientPairingStore

LOG = logging.getLogger("bridge")

# One clock for everything: the Sendspin client converts server time on it, and the
# Sonos position is fitted against it.
CLOCK = RawMonotonicClock()

SONOS_API_KEY = "123e4567-e89b-12d3-a456-426655440000"  # public key of the Sonos local API
# The Sonos is sent AAC at 320 kbps. It does not play a live WAV stream, and it drops a
# live FLAC stream while the bridge holds back writes after a cold start (tested on a
# Play:1, S2 86.10), so lossless is not an option for this stream.
ENCODER = ["-c:a", "aac", "-b:a", "320k", "-f", "adts"]
AAC_PRIMING = 1024        # frames ffmpeg's AAC encoder puts before the first input sample
BLOCK = 1024              # frames written per placement decision
PRIME_S = float(os.environ.get("BRIDGE_PRIME_S", "7.0"))  # near-silence written at once on a cold start
PRIME_HOLD_S = 4.0        # the Sonos plays 2.3-3 s after load; then it is fed in real time
LOW_WATER_S = 0.5         # below this much queued in the Sonos, write silence rather than wait
DRAIN_MARGIN_S = 0.3      # mute this much longer than the queue that is being dropped
SETTLE_S = 10.0           # after the Sonos (re)connects, its volume reports are its own boot state
VOLUME_POLL_S = 0.5      # how often the Sonos volume is read for changes made at the Sonos
RENDERING_CONTROL = "urn:schemas-upnp-org:service:RenderingControl:1"
DEADBAND_S = 0.0005       # placement error tolerated before slewing by one frame
JUMP_S = 0.020            # placement error that is corrected at once instead of slewed
GOOD_RTT_US = 30_000      # position replies slower than this are not used
FIT_WINDOW_US = 30_000_000
FIT_MIN_POINTS = 8
FIT_MIN_SPAN_US = 500_000
REFIT_ERROR_MS = 15.0     # this many consecutive misses mean the Sonos re-buffered
REFIT_MISSES = 5
SLOPE_MIN_SPAN_US = 120_000_000
SLOPE_WINDOW_US = 600_000_000
RTT_SLACK_US = 5_000      # replies slower than the fastest recent one by more are not used


@dataclass
class Options:
    sonos_ip: str = ""
    host_ip: str = ""         # the address the Sonos fetches the stream from; found if empty
    name: str = "Kitchen (sync)"
    server_url: str = "ws://127.0.0.1:8927/sendspin"
    http_port: int = 8199
    lead_ms: int = 2000       # Sendspin send-ahead the bridge asks for
    queue_ms: int = 1000      # audio held queued in the Sonos while playing
    idle_keep_s: int = 900    # keep the Sonos stream up this long after the last stream
    sonos_latency_ms: float = 0.0
    volume_trim_db: float = 0.0  # kitchen louder (+) or quieter (-) than the Echo at one number
    # The Sonos app (or the speaker's buttons) sets the kitchen's level rather than
    # its volume number: see Bridge._on_sonos_volume.
    sonos_app_sets_level: bool = True
    hold_mute: bool = False   # testing: keep the Sonos at volume 0 and muted
    data_dir: str = "/data"

    @classmethod
    def load(cls) -> Options:
        path = Path(os.environ.get("BRIDGE_OPTIONS", "/data/options.json"))
        raw = json.loads(path.read_text()) if path.exists() else {}
        for field in cls.__dataclass_fields__:
            env = os.environ.get("BRIDGE_" + field.upper())
            if env is not None:
                raw[field] = env
        opts = cls(**{k: v for k, v in raw.items() if k in cls.__dataclass_fields__})
        opts.http_port = int(opts.http_port)
        opts.lead_ms = int(opts.lead_ms)
        opts.queue_ms = int(opts.queue_ms)
        opts.idle_keep_s = int(opts.idle_keep_s)
        opts.sonos_latency_ms = float(opts.sonos_latency_ms)
        opts.volume_trim_db = float(opts.volume_trim_db)
        opts.hold_mute = str(opts.hold_mute).lower() in ("1", "true", "yes")
        opts.sonos_app_sets_level = str(opts.sonos_app_sets_level).lower() in ("1", "true", "yes")
        if not opts.sonos_ip:
            raise SystemExit("sonos_ip is not set")
        if not opts.host_ip:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
                probe.connect((opts.sonos_ip, 1400))   # no packet is sent
                opts.host_ip = probe.getsockname()[0]
        return opts


# The Sonos's own volume curve: its 0-100 against dB, relative to volume 45.
# Measured on a Play:1 (S2 86.10) with the Echo's raw microphone and a steady
# 1 kHz tone, 2026-10-19. Above 45 it rises about 0.58 dB a step.
SONOS_CURVE = [(5, -29.5), (8, -26.2), (10, -21.5), (13, -17.7), (16, -15.7), (20, -13.1),
               (25, -10.5), (30, -8.0), (35, -5.7), (40, -2.9), (45, 0.0)]
SONOS_DB_PER_STEP_ABOVE = 0.58
# Sendspin volume on the Echo Dot (biscuit-dsp) is 0.5 dB per point below 100,
# so every step of Spotify's or Music Assistant's slider is the same size; the
# bridge gives the Sonos the same law, so one number sounds equally loud on both. Music Assistant raises a group by moving
# each member toward 100 by the same fraction, which changes rooms equally only
# when their numbers are equal - so keep both rooms on one number and set the
# kitchen's level with volume_trim_db. Volume 100 is the Sonos at 45 plus the
# trim; nothing is capped, a positive trim reaches the Sonos's top.
DB_PER_POINT = 0.5


def volume_db(volume: int) -> float:
    """dB of a Sendspin volume on the Echo's law, 0 at 100."""
    return -DB_PER_POINT * (100 - min(100, volume))


def volume_from_db(db: float) -> int:
    """The Sendspin volume closest to a dB on the Echo's law."""
    return max(1, min(100, round(100 + db / DB_PER_POINT)))


def sonos_db(volume: int) -> float:
    """dB of a Sonos volume, relative to Sonos volume 45."""
    if volume <= 0:
        return -120.0
    if volume >= SONOS_CURVE[-1][0]:
        return (volume - SONOS_CURVE[-1][0]) * SONOS_DB_PER_STEP_ABOVE
    if volume <= SONOS_CURVE[0][0]:
        v0, d0 = SONOS_CURVE[0]
        return d0 - (v0 - volume) * 1.1     # the low end, about 1.1 dB a step
    for (v0, d0), (v1, d1) in zip(SONOS_CURVE, SONOS_CURVE[1:]):
        if v0 <= volume <= v1:
            return d0 + (d1 - d0) * (volume - v0) / (v1 - v0)
    return 0.0


def to_sonos(volume: int, trim_db: float) -> int:
    """Sendspin volume (perceived loudness) to the Sonos volume, on the Echo's law."""
    if volume <= 0:
        return 0
    want = volume_db(volume) + trim_db
    return max(1, min(range(1, 101), key=lambda v: abs(sonos_db(v) - want)))


def from_sonos(volume: int, trim_db: float) -> int:
    """Sonos volume to the Sendspin volume of the same loudness."""
    if volume <= 0:
        return 0
    return volume_from_db(sonos_db(volume) - trim_db)


class Timeline:
    """Received Sendspin PCM, addressed by absolute frame on the server clock."""

    def __init__(self) -> None:
        self.rate = 0
        self.channels = 2
        self._chunks: deque[tuple[int, np.ndarray]] = deque()
        self._end: int | None = None

    def reset(self, rate: int, channels: int) -> None:
        self.rate, self.channels = rate, channels
        self.clear()

    def clear(self) -> None:
        self._chunks.clear()
        self._end = None

    def frame_at(self, server_us: int) -> int:
        return round(server_us * self.rate / 1_000_000)

    @property
    def end(self) -> int | None:
        return self._end

    def add(self, server_us: int, pcm: bytes) -> None:
        if not self.rate:
            return
        data = np.frombuffer(pcm, dtype="<i2").reshape(-1, self.channels)
        start = self.frame_at(server_us)
        if self._end is not None and abs(start - self._end) <= 2:
            start = self._end  # rounding, not a gap
        self._chunks.append((start, data))
        self._end = start + len(data)

    def prune(self, before: int) -> None:
        while self._chunks and self._chunks[0][0] + len(self._chunks[0][1]) < before:
            self._chunks.popleft()

    def read(self, start: int, count: int) -> np.ndarray:
        out = np.zeros((count, self.channels), dtype="<i2")
        stop = start + count
        for c_start, data in self._chunks:
            c_stop = c_start + len(data)
            if c_stop <= start:
                continue
            if c_start >= stop:
                break
            lo, hi = max(start, c_start), min(stop, c_stop)
            out[lo - start:hi - start] = data[lo - c_start:hi - c_start]
        return out


class PositionFit:
    """Where the Sonos stream position stands against the local clock.

    The offset is the median over the last FIT_WINDOW_US of replies, so one slow
    reply cannot move it. The slope (the Sonos clock against this host's raw
    monotonic clock, a few ppm apart) is held fixed for that: fitted from a
    30 s window, 3 ms of reply jitter makes it wander by +-100 ppm, and the
    placement wandered with it. It is fitted only from one-per-second samples
    spanning SLOPE_MIN_SPAN_US, where the error is a few ppm, and persisted.
    """

    def __init__(self, slope: float = 0.001) -> None:
        self.points: deque[tuple[int, int]] = deque()
        self.long: deque[tuple[int, float]] = deque()   # (t_us, offset at slope 1) once a second
        self.slope = slope    # ms of stream per us of local clock
        self.offset = 0.0     # stream position (ms) at local time t0
        self.t0 = 0
        self.ready = False
        self._misses = 0

    def reset(self) -> None:
        self.points.clear()
        self.long.clear()
        self.ready = False
        self._misses = 0

    def add(self, t_us: int, pos_ms: int) -> None:
        if not self.points:
            self.t0 = t_us
        t_us -= self.t0
        if self.ready:
            error = pos_ms - (self.slope * t_us + self.offset)
            if abs(error) > REFIT_ERROR_MS:
                self._misses += 1
                if self._misses >= REFIT_MISSES:
                    LOG.warning("Sonos position moved by %.0f ms - refitting", error)
                    self.reset()
                return
            self._misses = 0
        self.points.append((t_us, pos_ms))
        while self.points and t_us - self.points[0][0] > FIT_WINDOW_US:
            self.points.popleft()
        if len(self.points) < FIT_MIN_POINTS or t_us - self.points[0][0] < FIT_MIN_SPAN_US:
            return
        t = np.array([q[0] for q in self.points], dtype=np.float64)
        p = np.array([q[1] for q in self.points], dtype=np.float64)
        self._track_slope(t_us, float(np.median(p - t / 1000)))
        self.offset = float(np.median(p - self.slope * t))
        if not self.ready:
            LOG.info("Sonos timeline locked (%d points)", len(self.points))
        self.ready = True

    def _track_slope(self, t_us: int, offset_at_unity: float) -> None:
        if self.long and t_us - self.long[-1][0] < 1_000_000:
            return
        self.long.append((t_us, offset_at_unity))
        while t_us - self.long[0][0] > SLOPE_WINDOW_US:
            self.long.popleft()
        if t_us - self.long[0][0] < SLOPE_MIN_SPAN_US:
            return
        t = np.array([q[0] for q in self.long], dtype=np.float64)
        o = np.array([q[1] for q in self.long], dtype=np.float64)
        drift, _ = np.polyfit(t - t[0], o, 1)     # ms of offset per us
        slope = 0.001 + drift
        if abs(slope * 1000 - 1) < 200e-6:
            self.slope = float(slope)

    def local_time_of(self, pos_ms: float) -> float:
        return self.t0 + (pos_ms - self.offset) / self.slope


class Sonos:
    """The parts of the Sonos local (websocket) API the bridge needs."""

    def __init__(self, ip: str, session: aiohttp.ClientSession) -> None:
        self.ip = ip
        self.session = session
        self._ssl = ssl.create_default_context()
        self._ssl.check_hostname = False
        self._ssl.verify_mode = ssl.CERT_NONE
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._ids = itertools.count(1)
        self._pending: dict[str, asyncio.Future] = {}
        self._reader: asyncio.Task | None = None
        self.household = self.group = self.player = ""
        self.volume_listener = None
        self.session_id: str | None = None
        # Set when the websocket to the Sonos drops (a reboot, the network); cleared
        # by the bridge once it has the Sonos playing the stream again.
        self.lost = False
        self.connected_at = float("-inf")

    async def _connect(self) -> None:
        if self._ws is not None and not self._ws.closed:
            return
        headers = {"X-Sonos-Api-Key": SONOS_API_KEY}
        async with self.session.get(f"https://{self.ip}:1443/api/v1/players/local/info",
                                    headers=headers, ssl=self._ssl) as resp:
            info = await resp.json()
        self.household, self.group, self.player = info["householdId"], info["groupId"], info["playerId"]
        self._ws = await self.session.ws_connect(info["websocketUrl"], headers=headers, ssl=self._ssl,
                                                 protocols=["v1.api.smartspeaker.audio"], heartbeat=30)
        self._reader = asyncio.create_task(self._read(self._ws))
        self.connected_at = time.monotonic()

    async def _read(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        async for msg in ws:
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue
            header, body = msg.json()
            fut = self._pending.pop(header.get("cmdId", ""), None)
            if fut is not None and not fut.done():
                fut.set_result((header, body))
            elif header.get("type") == "playerVolume" and self.volume_listener:
                self.volume_listener(body.get("volume", 0), body.get("muted", False))
            elif header.get("type") == "groupCoordinatorChanged" or header.get("namespace") == "groups:1":
                self.group = body.get("groupId", self.group)
        self.lost = True
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(ConnectionError("Sonos websocket closed"))
        self._pending.clear()

    async def _send(self, namespace: str, command: str, body: dict, **target) -> dict:
        assert self._ws is not None
        cmd_id = str(next(self._ids))
        fut = asyncio.get_running_loop().create_future()
        self._pending[cmd_id] = fut
        header = {"namespace": namespace, "command": command, "householdId": self.household,
                  "cmdId": cmd_id, **target}
        await self._ws.send_json([header, body])
        reply_header, reply = await asyncio.wait_for(fut, 5)
        if reply_header.get("success") is False:
            raise RuntimeError(f"Sonos {command} failed: {reply}")
        return reply

    async def ensure(self) -> None:
        """Connect, so household, group and player ids are known before they are used."""
        try:
            await self._connect()
        except (aiohttp.ClientError, ConnectionError, asyncio.TimeoutError):
            if self._ws is not None:
                await self._ws.close()
            raise

    async def call(self, namespace: str, command: str, body: dict | None = None, **target) -> dict:
        try:
            await self._connect()
            return await self._send(namespace, command, body or {}, **target)
        except (aiohttp.ClientError, ConnectionError, asyncio.TimeoutError):
            if self._ws is not None:
                await self._ws.close()
            raise

    async def play_url(self, url: str, title: str) -> None:
        await self.ensure()
        session = await self.call("playbackSession:1", "createSession",
                                  {"appId": "io.sendspin.bridge", "appContext": "1"}, groupId=self.group)
        self.session_id = session["sessionId"]
        await self.call("playbackSession:1", "loadStreamUrl", {
            "streamUrl": url, "playOnCompletion": True,
            "stationMetadata": {"_objectType": "container", "name": title, "type": "station"},
        }, sessionId=self.session_id)

    async def stop(self) -> None:
        with contextlib.suppress(Exception):
            await self.ensure()
        with contextlib.suppress(Exception):
            await self.call("playback:1", "pause", groupId=self.group)
        if self.session_id:
            with contextlib.suppress(Exception):
                await self.call("playbackSession:1", "suspend", sessionId=self.session_id)
        self.session_id = None

    async def status(self) -> tuple[int, int, int, str]:
        """(sent_us, received_us, positionMillis, playbackState) on CLOCK."""
        await self.ensure()
        sent = CLOCK.now_us()
        reply = await self.call("playback:1", "getPlaybackStatus", groupId=self.group)
        received = CLOCK.now_us()
        return sent, received, int(reply.get("positionMillis") or 0), reply.get("playbackState", "")

    # Volume and mute go over UPnP RenderingControl (port 1400), not the websocket
    # API: since 2026-10-19 the Sonos answers playerVolume:1 - get, set and
    # subscribe alike - with ERROR_NO_PERMISSION "Credential is not allowed" for
    # this API key, while playback on the same connection still works. UPnP is
    # what Home Assistant's own Sonos integration uses. Changes are polled.

    async def _upnp(self, action: str, args: str = "") -> str:
        body = ('<?xml version="1.0"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" '
                's:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"><s:Body>'
                f'<u:{action} xmlns:u="{RENDERING_CONTROL}"><InstanceID>0</InstanceID>'
                f'<Channel>Master</Channel>{args}</u:{action}></s:Body></s:Envelope>')
        headers = {"Content-Type": 'text/xml; charset="utf-8"',
                   "SOAPACTION": f'"{RENDERING_CONTROL}#{action}"'}
        async with self.session.post(f"http://{self.ip}:1400/MediaRenderer/RenderingControl/Control",
                                     data=body, headers=headers,
                                     timeout=aiohttp.ClientTimeout(total=5)) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"Sonos {action} failed: HTTP {resp.status}")
            return text

    async def volume(self) -> tuple[int, bool]:
        vol = re.search(r"<CurrentVolume>(\d+)</CurrentVolume>", await self._upnp("GetVolume"))
        mute = re.search(r"<CurrentMute>(\d)</CurrentMute>", await self._upnp("GetMute"))
        if vol is None or mute is None:
            raise RuntimeError("Sonos volume reply not understood")
        return int(vol.group(1)), mute.group(1) == "1"

    async def set_volume(self, volume: int) -> None:
        await self._upnp("SetVolume", f"<DesiredVolume>{int(volume)}</DesiredVolume>")

    async def set_mute(self, muted: bool) -> None:
        await self._upnp("SetMute", f"<DesiredMute>{1 if muted else 0}</DesiredMute>")

    async def watch_volume(self) -> None:
        """Report volume and mute changes to volume_listener, as the subscription did."""
        last: tuple[int, bool] | None = None
        while True:
            await asyncio.sleep(VOLUME_POLL_S)
            try:
                current = await self.volume()
            except Exception as err:
                LOG.debug("Volume poll failed: %s", err)
                # unreachable (a reboot): the first reading after is where the
                # Sonos came up, not someone turning it - _watch_sonos sets it back
                last = None
                continue
            if last is not None and current != last and self.volume_listener:
                self.volume_listener(*current)
            last = current


def quiet(frames: int, channels: int) -> np.ndarray:
    """Silence with +-1 LSB of dither (-90 dBFS): digital zeros encode to almost no
    data, and a Sonos waits for data before it starts and drops a stream that dries up."""
    return np.random.randint(-1, 2, size=(frames, channels)).astype("<i2")


class Feed:
    """One HTTP connection from the Sonos: a live AAC stream placed on the timeline."""

    def __init__(self, bridge: Bridge, resp: web.StreamResponse) -> None:
        self.bridge = bridge
        self.resp = resp
        self.rate = bridge.timeline.rate
        self.channels = bridge.timeline.channels
        self.written = 0                  # frames given to the encoder
        self.next_frame: int | None = None  # next timeline frame to take
        self.started_us = CLOCK.now_us()
        self.closed = asyncio.Event()
        self.slewed = 0
        self._placed_logged = False
        self.jumps = 0
        self.gaps = 0
        self.ahead_s = 0.0

    async def run(self) -> None:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-f", "s16le", "-ar", str(self.rate), "-ac", str(self.channels), "-i", "pipe:0",
            *ENCODER, "-flush_packets", "1", "pipe:1",
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
        pump = asyncio.create_task(self._pump(proc))
        try:
            while not self.closed.is_set() and not pump.done():
                block = self._next_blocks()
                if block is not None:
                    proc.stdin.write(block)
                    await proc.stdin.drain()
                else:
                    await asyncio.sleep(0.01)
        finally:
            self.closed.set()
            with contextlib.suppress(Exception):
                proc.kill()
            pump.cancel()

    async def _pump(self, proc: asyncio.subprocess.Process) -> None:
        try:
            while chunk := await proc.stdout.read(4096):
                await self.resp.write(chunk)
        except (ConnectionResetError, ConnectionError):
            pass
        finally:
            self.closed.set()

    def queued_s(self) -> float:
        """Seconds of audio the Sonos has been given but not yet played."""
        if not self.bridge.fit.ready:
            return 0.0
        return max(0.0, (self._play_time_of(self.written) - CLOCK.now_us()) / 1e6)

    def _play_time_of(self, frame: int) -> float:
        """Local time (us) at which the Sonos plays this stream frame."""
        pos_ms = (frame + AAC_PRIMING) * 1000 / self.rate
        return self.bridge.fit.local_time_of(pos_ms)

    def _next_blocks(self) -> bytes | None:
        """Up to 200 ms of audio that may be written now, or None to wait."""
        b = self.bridge
        now = CLOCK.now_us()
        out = []
        for _ in range(10):
            if not b.fit.ready or b.current_feed is not self:
                # not placed yet: the priming burst, then nothing more while the Sonos
                # starts on it (anything more would only delay the first placed audio),
                # and real time only if it is slow to start
                elapsed = (now - self.started_us) / 1e6
                due = self.rate * (PRIME_S + max(0.0, elapsed - PRIME_HOLD_S))
                if self.written >= due:
                    break
                self.next_frame = None
                out.append(quiet(BLOCK, self.channels))
                self.written += BLOCK
                continue
            play_us = self._play_time_of(self.written)
            ahead_s = (play_us - now) / 1e6
            if not self._placed_logged:
                self._placed_logged = True
                LOG.info("First placement: %.2f s written, plays in %.2f s", self.written / self.rate, ahead_s)
            if ahead_s > b.opts.queue_ms / 1000:
                break
            server_us = b.client.compute_server_time(int(play_us + b.opts.sonos_latency_ms * 1000))
            target = b.timeline.frame_at(server_us)
            if self.next_frame is None or abs(target - self.next_frame) > JUMP_S * self.rate:
                if self.next_frame is not None:
                    LOG.info("Placement off by %.1f ms - jumping",
                             (target - self.next_frame) * 1000 / self.rate)
                    self.jumps += 1
                self.next_frame = target
            elif target - self.next_frame > DEADBAND_S * self.rate:
                self.next_frame += 1   # Sonos runs ahead: drop a frame
                self.slewed += 1
            elif self.next_frame - target > DEADBAND_S * self.rate:
                self.next_frame -= 1   # Sonos runs behind: repeat a frame
                self.slewed -= 1
            end = b.timeline.end
            have = end is not None and end >= self.next_frame + BLOCK
            if not have and ahead_s > LOW_WATER_S:
                break   # wait for the server rather than write a gap
            self.ahead_s = ahead_s
            block = b.timeline.read(self.next_frame, BLOCK)
            if not have and b.streaming:
                self.gaps += 1
            if not block.any():
                # digital silence, received or a gap: FLAC codes it in a few bytes, and a
                # Sonos whose stream dries up like that drops the connection
                block = quiet(BLOCK, self.channels)
            out.append(block)
            self.next_frame += BLOCK
            self.written += BLOCK
            b.timeline.prune(self.next_frame - self.rate)
        if not out:
            return None
        return np.concatenate(out).tobytes()


class Bridge:
    def __init__(self, opts: Options) -> None:
        self.opts = opts
        self.timeline = Timeline()
        self.fit = PositionFit()
        self.loaded_us = 0
        self.client: SendspinClient
        self.sonos: Sonos
        self.session: aiohttp.ClientSession
        self.current_feed: Feed | None = None
        self.streaming = False
        self._starting = False
        self._idle_task: asyncio.Task | None = None
        self._unmute_task: asyncio.Task | None = None
        self.generation = 0
        self.volume = (50, False)     # Sendspin volume (perceived loudness), muted
        self._sent_level: int | None = None
        # Levels the bridge itself set, with when. The Sonos confirms each one,
        # but late and sometimes after a newer one, so only remembering the last
        # would take the confirmation of an older one for someone at the Sonos.
        self._sent_at: dict[int, float] = {}
        self._tasks: set[asyncio.Task] = set()
        self._disconnected = asyncio.Event()
        self._state_path = Path(opts.data_dir) / "state.json"

    # --- persistence -------------------------------------------------------

    def _state(self) -> dict:
        try:
            return json.loads(self._state_path.read_text())
        except (OSError, ValueError):
            return {}

    def _save_state(self, **changes) -> None:
        state = self._state() | changes
        self._state_path.write_text(json.dumps(state))

    def _identity(self) -> Identity:
        path = Path(self.opts.data_dir) / "identity.key"
        try:
            return Identity.from_private_bytes(b64url_decode(path.read_text().strip()))
        except FileNotFoundError:
            identity = Identity.generate()
            path.write_text(identity.private_b64u)
            path.chmod(0o600)
            return identity

    # --- lifecycle ---------------------------------------------------------

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def run(self) -> None:
        self.session = aiohttp.ClientSession()
        self.sonos = Sonos(self.opts.sonos_ip, self.session)
        if slope := self._state().get("slope"):
            self.fit.slope = float(slope)
        if self.opts.sonos_app_sets_level and "level_db" in self._state():
            self.opts.volume_trim_db = float(self._state()["level_db"])
            LOG.info("Level %+.1f dB (set from the Sonos app)", self.opts.volume_trim_db)
        try:
            level, muted = await self.sonos.volume()
            # The number Music Assistant last gave, if the Sonos is still where
            # the bridge left it; recomputing it from the level could move it
            # by one and pull the rooms apart.
            saved = self._state().get("volume")
            if isinstance(saved, int) and to_sonos(saved, self.opts.volume_trim_db) == level:
                self.volume = (saved, muted)
            else:
                self.volume = (from_sonos(level, self.opts.volume_trim_db), muted)
            self._sent_level = level
        except Exception as err:  # Sonos away at start: report a default, learn it later
            LOG.warning("Sonos not reachable yet: %s", err)
        # Only now: the Sonos reports its volume as soon as the bridge connects,
        # and taken against the default volume that read as someone at the
        # Sonos setting a new level.
        self.sonos.volume_listener = self._on_sonos_volume
        self._spawn(self.sonos.watch_volume())

        app = web.Application()
        app.router.add_get("/stream.aac", self._serve)
        app.router.add_get("/silence.wav", self._silence)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", self.opts.http_port).start()

        store = await FileClientPairingStore.open(Path(self.opts.data_dir) / "pairing.json")
        self.client = SendspinClient(
            self._identity(), self.opts.name, [Roles.PLAYER],
            pairing_store=store,
            device_info=DeviceInfo(product_name="Sonos via Sendspin bridge", manufacturer="Sonos",
                                   software_version="0.1"),
            player_support=ClientHelloPlayerSupport(
                supported_formats=[
                    SupportedAudioFormat(codec=AudioCodec.PCM, channels=2, sample_rate=48000, bit_depth=16),
                    SupportedAudioFormat(codec=AudioCodec.PCM, channels=2, sample_rate=44100, bit_depth=16),
                ],
                buffer_capacity=8_000_000,
            ),
            session=self.session,
            clock=CLOCK,
            output_delay_ms=float(self._state().get("output_delay_ms", 0)),
            required_lead_time_ms=self.opts.lead_ms,
            min_buffer_ms=self.opts.lead_ms,
            initial_volume=self.volume[0],
            initial_muted=self.volume[1],
            state_supported_commands=[PlayerCommand.VOLUME, PlayerCommand.MUTE],
        )
        await self.client.set_unpaired_access(enabled=True)
        self.client.add_stream_start_listener(self._on_stream_start)
        self.client.add_stream_clear_listener(self._on_stream_clear)
        self.client.add_stream_end_listener(self._on_stream_end)
        self.client.add_audio_chunk_listener(self._on_chunk)
        self.client.add_server_command_listener(self._on_command)
        self.client.add_output_delay_listener(lambda ms: self._save_state(output_delay_ms=ms))
        self.client.add_disconnect_listener(self._disconnected.set)
        self._spawn(self._poll_position())
        self._spawn(self._report())
        self._spawn(self._watch_sonos())

        while True:
            self._disconnected.clear()
            try:
                await self.client.connect(self.opts.server_url)
                LOG.info("Connected to %s as %r", self.opts.server_url, self.opts.name)
                await self._disconnected.wait()
                LOG.warning("Disconnected from the Sendspin server")
            except Exception as err:
                LOG.warning("Cannot connect to %s: %s", self.opts.server_url, err)
            await self._stop_sonos()
            await asyncio.sleep(5)

    # --- Sendspin events -----------------------------------------------------

    def _on_stream_start(self, message) -> None:
        player = message.payload.player
        if player is None:
            return
        if player.codec is not AudioCodec.PCM or player.bit_depth != 16:
            LOG.error("Unexpected stream format %s/%s", player.codec, player.bit_depth)
            return
        self.streaming = True
        self._cancel_idle()
        if (player.sample_rate, player.channels) != (self.timeline.rate, self.timeline.channels):
            # the encoder runs at the stream rate: a new rate needs a new Sonos stream
            self.timeline.reset(player.sample_rate, player.channels)
            LOG.info("Stream start %d Hz: starting the Sonos stream", player.sample_rate)
            self._spawn(self._start_sonos())
        elif self.current_feed is None and not self._starting:
            LOG.info("Stream start: starting the Sonos stream")
            self._spawn(self._start_sonos())
        else:
            LOG.info("Stream start: Sonos stream already running")

    def _on_stream_clear(self, roles) -> None:
        if roles is not None and "player" not in roles:
            return
        LOG.info("Stream clear")
        self.timeline.clear()
        self._drop_queued()

    def _on_stream_end(self, roles) -> None:
        if roles is not None and "player" not in roles:
            return
        LOG.info("Stream end")
        self.streaming = False
        self.timeline.clear()
        self._drop_queued()
        self._cancel_idle()
        self._idle_task = asyncio.create_task(self._idle_stop())

    def _drop_queued(self) -> None:
        """Silence what the Sonos still has queued: mute it until that has played."""
        feed = self.current_feed
        if feed is None or not self.fit.ready or self.opts.hold_mute or self.volume[1]:
            return
        if self._unmute_task is not None:
            self._unmute_task.cancel()
        else:
            self._spawn(self._apply(self.sonos.set_mute(True)))
        self._unmute_task = asyncio.create_task(self._unmute_after(feed.queued_s() + DRAIN_MARGIN_S))

    async def _unmute_after(self, seconds: float) -> None:
        try:
            await asyncio.sleep(seconds)
        except asyncio.CancelledError:
            return
        self._unmute_task = None
        if not self.volume[1]:
            await self._apply(self.sonos.set_mute(False))

    def _cancel_idle(self) -> None:
        if self._idle_task is not None:
            self._idle_task.cancel()
            self._idle_task = None

    async def _idle_stop(self) -> None:
        try:
            await asyncio.sleep(self.opts.idle_keep_s)
        except asyncio.CancelledError:
            return
        self._idle_task = None
        LOG.info("Idle for %d s: stopping the Sonos stream", self.opts.idle_keep_s)
        await self._stop_sonos()

    def _on_chunk(self, server_us: int, data: bytes, fmt, _send_ahead: int) -> None:
        self.timeline.add(server_us, data)

    def _on_command(self, payload) -> None:
        cmd = payload.player
        if cmd is None:
            return
        if self.opts.hold_mute:
            LOG.info("hold_mute: ignoring %s", cmd.command)
            return
        if cmd.command is PlayerCommand.VOLUME and cmd.volume is not None:
            level = to_sonos(cmd.volume, self.opts.volume_trim_db)
            self._sent_level = level
            self._sent_at[level] = time.monotonic()
            self._save_state(volume=int(cmd.volume))
            self._spawn(self._apply(self.sonos.set_volume(level)))
            self.volume = (cmd.volume, self.volume[1])
        elif cmd.command is PlayerCommand.MUTE and cmd.mute is not None:
            self._spawn(self._apply(self.sonos.set_mute(cmd.mute)))
            self.volume = (self.volume[0], cmd.mute)
        else:
            return
        self._spawn(self._report_volume())

    async def _apply(self, coro) -> None:
        try:
            await coro
        except Exception as err:
            LOG.warning("Sonos volume change failed: %s", err)

    def _on_sonos_volume(self, level: int, muted: bool) -> None:
        if self._unmute_task is not None:
            muted = self.volume[1]   # the bridge's own drain mute, not the user's
        # the Sonos echoes the level the bridge just set: keep the Sendspin volume
        # it came from, which a round trip through the curve could move by one
        now = time.monotonic()
        self._sent_at = {k: t for k, t in self._sent_at.items() if now - t < 5.0}
        if level == self._sent_level or level in self._sent_at:
            volume = self.volume[0]
        elif now - self.sonos.connected_at < SETTLE_S:
            # The Sonos just (re)connected - after a reboot it reports the volume
            # it booted with, several times. That is nobody's choice: keep the
            # level, and _recover_sonos puts the Sonos back where it was.
            LOG.info("Sonos reports volume %d while reconnecting: not a new level", level)
            volume = self.volume[0]
        elif self.opts.sonos_app_sets_level and level > 0 and self.volume[0] > 0                 and not self.opts.hold_mute:
            # Someone turned the Sonos itself. That is the kitchen's level, not
            # the group's volume: keep the number Music Assistant sees - both
            # rooms stay on one number, which its group volume needs to move
            # them by the same amount - and remember how far the kitchen now
            # sits from it. The next volume from Spotify or Music Assistant
            # keeps that offset instead of resetting it.
            trim = sonos_db(level) - volume_db(self.volume[0])
            self.opts.volume_trim_db = max(-60.0, min(40.0, trim))
            self._save_state(level_db=round(self.opts.volume_trim_db, 2))
            self._sent_level = level
            LOG.info("Level %+.1f dB (Sonos app: %d at volume %d)",
                     self.opts.volume_trim_db, level, self.volume[0])
            volume = self.volume[0]
        else:
            volume = from_sonos(level, self.opts.volume_trim_db)
            self._sent_level = None
        if (volume, muted) != self.volume:
            self.volume = (volume, muted)
            self._spawn(self._report_volume())

    async def _report_volume(self) -> None:
        with contextlib.suppress(Exception):
            await self.client.send_player_state(available=True, volume=self.volume[0], muted=self.volume[1])

    # --- Sonos --------------------------------------------------------------

    async def _start_sonos(self) -> None:
        self._starting = True
        try:
            await self._load_sonos()
        finally:
            self._starting = False

    async def _load_sonos(self) -> None:
        self.generation += 1
        self.fit.reset()
        if self.current_feed is not None:
            self.current_feed.closed.set()
            self.current_feed = None
        url = f"http://{self.opts.host_ip}:{self.opts.http_port}/stream.aac?g={self.generation}"
        try:
            if self.opts.hold_mute:
                await self.sonos.set_volume(0)
                await self.sonos.set_mute(True)
                for _ in range(30):
                    if await self.sonos.volume() == (0, True):
                        break
                    await asyncio.sleep(0.1)
                else:
                    raise RuntimeError("hold_mute: the Sonos did not go silent, not starting")
            await self.sonos.play_url(url, self.opts.name)
            self.loaded_us = CLOCK.now_us()
            LOG.info("Sonos loading %s", url)
        except Exception as err:
            LOG.error("Sonos would not start the stream: %s", err)

    async def _stop_sonos(self) -> None:
        self.generation += 1
        self.fit.reset()
        if self.current_feed is not None:
            self.current_feed.closed.set()
            self.current_feed = None
        await self.sonos.stop()

    async def _silence(self, _request: web.Request) -> web.Response:
        """Ten minutes of silence, for testing without sound."""
        import struct
        frames = 8000 * 600
        header = b"RIFF" + struct.pack("<I", 36 + frames * 2) + b"WAVEfmt " + struct.pack(
            "<IHHIIHH", 16, 1, 1, 8000, 16000, 2, 16) + b"data" + struct.pack("<I", frames * 2)
        return web.Response(body=header + bytes(frames * 2), content_type="audio/wav")

    async def _serve(self, request: web.Request) -> web.StreamResponse:
        if request.query.get("g") != str(self.generation):
            return web.Response(status=410)
        resp = web.StreamResponse(headers={"Content-Type": "audio/aac", "Cache-Control": "no-cache"})
        await resp.prepare(request)
        feed = Feed(self, resp)
        if self.current_feed is not None:
            self.current_feed.closed.set()  # the Sonos opened a new connection: this one counts
        self.current_feed = feed
        self.fit.reset()
        LOG.info("Sonos connected for stream %s", request.query.get("g"))
        await feed.run()
        if self.current_feed is feed:
            self.current_feed = None
        return resp

    async def _watch_sonos(self) -> None:
        """Bring the stream back when the Sonos returns from a reboot or a network drop.

        Only after the connection to the Sonos was lost: a Sonos that is still
        there but playing something else was switched by someone, and is left
        alone. The Sonos is reconnected when the bridge next talks to it, which
        it otherwise only does while a stream is up - hence the probing here.
        """
        while True:
            await asyncio.sleep(2)
            if not self.sonos.lost or not self.streaming or self._starting:
                continue
            try:
                await self.sonos.ensure()
            except Exception as err:
                LOG.debug("Sonos still away: %s", err)
                continue
            self.sonos.lost = False
            LOG.info("Sonos is back: restarting the stream")
            level = to_sonos(self.volume[0], self.opts.volume_trim_db)
            self._sent_level = level
            self._sent_at[level] = time.monotonic()
            await self._apply(self.sonos.set_volume(level))
            if self.current_feed is not None:
                self.current_feed.closed.set()
                self.current_feed = None
            await self._start_sonos()

    async def _report(self) -> None:
        while True:
            await asyncio.sleep(10)
            feed = self.current_feed
            if feed is None or not self.fit.ready:
                continue
            LOG.info("Sonos queue %.2f s, drift %+.1f ppm, slewed %+d frames, %d jumps, %d gap blocks",
                     feed.ahead_s, (self.fit.slope * 1000 - 1) * 1e6, feed.slewed, feed.jumps, feed.gaps)
            self._save_state(slope=self.fit.slope)

    async def _poll_position(self) -> None:
        rtts: deque[int] = deque()
        while True:
            if self.current_feed is None:
                await asyncio.sleep(0.1)
                continue
            try:
                sent, received, pos, state = await self.sonos.status()
            except Exception as err:
                LOG.debug("Position poll failed: %s", err)
                await asyncio.sleep(0.5)
                continue
            rtt = received - sent
            rtts.append(rtt)
            if len(rtts) > 100:
                rtts.popleft()
            if state == "PLAYBACK_STATE_PLAYING" and rtt < min(GOOD_RTT_US, min(rtts) + RTT_SLACK_US) and pos > 0:
                if not self.fit.points and self.loaded_us:
                    LOG.info("Sonos playing %.2f s after load", (sent - self.loaded_us) / 1e6 - pos / 1000)
                    self.loaded_us = 0
                self.fit.add((sent + received) // 2, pos)
            elif state != "PLAYBACK_STATE_PLAYING" and self.fit.ready:
                LOG.warning("Sonos left playback (%s)", state)
                self.fit.reset()
            await asyncio.sleep(0.1)


def main() -> None:
    logging.basicConfig(level=os.environ.get("BRIDGE_LOG", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    asyncio.run(Bridge(Options.load()).run())


if __name__ == "__main__":
    main()
