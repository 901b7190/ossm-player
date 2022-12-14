import asyncio
import bisect
import concurrent.futures
import dataclasses
import itertools
import json
import logging
import math
import signal
import socket
import time
import tkinter
import tkinter.ttk
import typing

import click
import mpv
import scipy.interpolate
import websockets.client


logger = logging.getLogger("ossm-player")


InterpolatorLiteral = typing.Literal["linear", "hermite"]


@dataclasses.dataclass
class Config:
    host: str
    video: str
    script_path: str
    min_depth: float
    max_depth: float
    delay: int
    buffer_size: int
    preload_ms: int
    interpolator: InterpolatorLiteral


class FunscriptAction(typing.TypedDict):
    pos: int
    at: int


class Funscript(typing.TypedDict):
    version: typing.Literal["1.0"]
    actions: typing.List[FunscriptAction]
    inverted: bool


@dataclasses.dataclass
class OSSMCommand:
    depth: float
    speed: float
    time: int


T = typing.TypeVar("T", int, float)


def scale(value: T, min_in: T, max_in: T, min_out: T, max_out: T) -> float:
    return ((max_out - min_out) * (value - min_in) / (max_in - min_in)) + min_out


@dataclasses.dataclass
class Frame:
    depth: float
    speed: float
    time: int


class Interpolator:
    def __init__(self, *, funscript: Funscript, config: Config):
        actions = funscript["actions"]
        self._config = config
        self._timecodes = [action["at"] for action in actions]
        self._positions = [
            scale(action["pos"], 0, 100, config.min_depth, config.max_depth)
            for action in actions
        ]

    def keyframes(self) -> typing.Generator[typing.Tuple[int, float], None, None]:
        for i, timecode_ms in enumerate(self._timecodes):
            yield timecode_ms, self._positions[i]

    def get_frames_at_timecode(self, timecode_ms: int) -> typing.List[Frame]:
        raise NotImplementedError()

    def reset(self) -> None:
        pass


class LinearInterpolator(Interpolator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._last_frame_idx_sent = -1

        self._speeds: typing.List[float] = []
        for i, current_at in enumerate(self._timecodes[:-1]):
            next_at = self._timecodes[i + 1]

            current_depth = self._positions[i]
            next_depth = self._positions[i + 1]

            speed = 0
            if next_at != current_at:
                speed = abs(
                    (next_depth - current_depth) / ((next_at - current_at) / 1000.0)
                )
            self._speeds.append(speed)
        self._speeds.append(0)

    def _idx_by_timecode(self, timecode_ms: int) -> int:
        return bisect.bisect(self._timecodes, timecode_ms)

    def get_frames_at_timecode(self, timecode_ms: int) -> typing.List[Frame]:
        result: typing.List[Frame] = []

        current_frame_idx = self._idx_by_timecode(timecode_ms)

        first_frame_to_send = max(current_frame_idx, self._last_frame_idx_sent + 1)
        last_frame_to_send = min(
            current_frame_idx + self._config.buffer_size, len(self._timecodes) - 1
        )
        for frame_idx in range(first_frame_to_send, last_frame_to_send):
            if self._config.preload_ms <= self._timecodes[frame_idx] - timecode_ms:
                break

            result.append(
                Frame(
                    depth=self._positions[frame_idx],
                    speed=self._speeds[frame_idx],
                    time=self._timecodes[frame_idx],
                )
            )
            self._last_frame_idx_sent = frame_idx

        return result

    def reset(self) -> None:
        super().reset()
        self._last_frame_idx_sent = 0


class CFRInterpolator(Interpolator):
    """Constant Frame Rate"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._last_frame_timecode_ms = 0

    def get_frames_at_timecode(self, timecode_ms: int) -> typing.List[Frame]:
        result: typing.List[Frame] = []

        pitch = int(self._config.preload_ms / self._config.buffer_size)
        for frame_timecode_ms in range(
            max(timecode_ms, self._last_frame_timecode_ms),
            timecode_ms + self._config.preload_ms,
            pitch,
        ):
            result.append(
                Frame(
                    depth=self.depth_at(frame_timecode_ms),
                    speed=self.speed_at(frame_timecode_ms),
                    time=frame_timecode_ms,
                )
            )
            self._last_frame_timecode_ms = frame_timecode_ms

        return result

    def reset(self) -> None:
        super().reset()
        self._last_frame_timecode_ms = 0

    def depth_at(self, timecode_ms: int) -> float:
        raise NotImplementedError()

    def speed_at(self, timecode_ms: int) -> float:
        raise NotImplementedError()


class HermiteInterpolator(CFRInterpolator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._depth = scipy.interpolate.pchip(
            self._timecodes,
            self._positions,
            extrapolate=False,
        )
        self._speed = self._depth.derivative()

    class _Floatable(typing.Protocol):
        def __float__(self) -> float:
            ...

    def _cast_float(self, val: _Floatable) -> float:
        if math.isnan(val):
            return 0.0
        return float(val)

    def depth_at(self, timecode_ms: int) -> float:
        return self._cast_float(self._depth(timecode_ms))

    def speed_at(self, timecode_ms: int) -> float:
        return abs(self._cast_float(self._speed(timecode_ms)) * 1000)


class OSSMController:
    def __init__(self, *, config: Config):
        self.config: Config = config

        self._mut: asyncio.Lock = asyncio.Lock()
        self._frames_producer: typing.Optional[asyncio.Task[None]] = None

        funscript: Funscript = load_funscript(config.script_path)
        self._interpolator = {
            "linear": LinearInterpolator,
            "hermite": HermiteInterpolator,
        }[config.interpolator](funscript=funscript, config=config)

        self._conn: websockets.client.connect
        self._client: websockets.client.WebSocketClientProtocol
        self._beginning_of_time: float

    async def __aenter__(self):
        self._conn = websockets.client.connect(
            f"ws://{self.config.host}/ws",
            # FIXME: ESP32 misses ping frames under high load?
            ping_interval=None,
        )
        self._client = await self._conn.__aenter__()

        await self._send_and_wait_for_ack({"type": "start_streaming"})
        await self._clear_frames()

        self._beginning_of_time = time.time() * 1000
        await self._send_frame(
            Frame(
                depth=next(self._interpolator.keyframes())[1],
                speed=100,
                time=0,
            )
        )

        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.reset()
        await self._send_and_wait_for_ack({"type": "stop"})
        await self._conn.__aexit__(exc_type, exc, tb)

    async def reset(self):
        if self._frames_producer:
            producer = self._frames_producer
            self._frames_producer = None
            producer.cancel()
            try:
                await producer
            except asyncio.CancelledError:
                pass

        self._interpolator.reset()
        await self._clear_frames()

    async def send_frames_at(self, timecode_ms: int):
        if self._frames_producer:
            return
        self._frames_producer = asyncio.create_task(
            self._send_frames_at(timecode_ms, now=time.time()),
        )

    async def _send_frames_at(self, timecode_ms: int, now: float):
        timecode_ms += self.config.delay

        for frame in self._interpolator.get_frames_at_timecode(timecode_ms):
            frame.time = int(
                now * 1000 - self._beginning_of_time + frame.time - timecode_ms
            )
            await self._send_frame(frame)

        self._frames_producer = None

    async def _wait_for_ack(self):
        while True:
            msg = json.loads(await self._client.recv())
            if msg["type"] == "ack":
                break
            elif "error" in msg:
                raise RuntimeError(f"Error while waiting for ack: {msg['error']}.")

    async def _send_and_wait_for_ack(self, data: typing.Dict, max_retries=4):
        for n_retry in itertools.count():
            async with self._mut:
                await self._client.send(json.dumps(data))
                try:
                    await asyncio.wait_for(self._wait_for_ack(), timeout=0.5)
                except asyncio.TimeoutError as exc:
                    if max_retries <= n_retry:
                        raise exc
                    logger.exception("Didn't receive ACK in time. Retrying.")
                else:
                    break

    async def _send_frame(self, frame: Frame):
        logger.info(f"sending frame {frame.depth=} {frame.speed=} {frame.time=}")
        await self._send_and_wait_for_ack(
            {
                "type": "send_frame",
                "depth": frame.depth,
                "speed": frame.speed,
                "time": frame.time,
            },
        )

    async def _clear_frames(self):
        async with self._mut:
            await self._client.send(json.dumps({"type": "clear_frames"}))
            await self._wait_for_ack()


def load_funscript(path: str) -> Funscript:
    funscript: Funscript = json.loads(open(path).read())
    funscript["actions"] = sorted(funscript["actions"], key=lambda action: action["at"])
    if funscript.get("inverted"):
        funscript["inverted"] = False
        funscript["actions"] = [
            FunscriptAction(at=action["at"], pos=100 - action["pos"])
            for action in funscript["actions"]
        ]
    return funscript


class OSSMGui:
    def __init__(
        self, ossm_controller: OSSMController, loop: asyncio.AbstractEventLoop
    ) -> None:
        self._root: tkinter.Tk
        self._ossm_controller = ossm_controller
        self._loop = loop
        self._running = True

    def close(self):
        self._running = False

    def run(self):
        self._root = root = tkinter.Tk()
        root.attributes("-type", "dialog")
        root.title("OSSM Streaming Control")
        root.protocol("WM_DELETE_WINDOW", self.close)
        frame = tkinter.ttk.Frame(root, padding=10)
        frame.grid()

        tkinter.ttk.Button(frame, text="Quit", command=self.close).grid(column=0, row=0)

        delay_label = tkinter.StringVar()
        tkinter.ttk.Label(frame, textvariable=delay_label).grid(column=1, row=0)

        def _delay_to_str(delay: int):
            delay_str = str(delay)
            if 0 <= delay:
                delay_str = "+" + delay_str
            return delay_str

        for i, delay in enumerate([-100, -10, 10, 100]):

            def _update_delay(delay: int):
                def _wrapped():
                    self._ossm_controller.config.delay += delay
                    asyncio.run_coroutine_threadsafe(
                        self._ossm_controller.reset(), self._loop
                    ).result()

                return _wrapped

            tkinter.ttk.Button(
                frame,
                text=f"Delay {_delay_to_str(delay)}ms",
                command=_update_delay(delay),
            ).grid(column=i, row=1)

        while self._running:
            delay_label.set(
                f"Delay: {_delay_to_str(self._ossm_controller.config.delay)}ms."
            )
            self._root.update()
            time.sleep(0.1)


async def play(config: Config):
    loop = asyncio.get_running_loop()

    async with OSSMController(config=config) as ossm_controller:
        player = mpv.MPV(
            input_default_bindings=True,
            input_vo_keyboard=True,
            osc=True,
            ytdl=True,
        )

        @player.property_observer("pause")
        def _pause_observer(_name, value: bool):
            if value is False:
                return

            asyncio.run_coroutine_threadsafe(ossm_controller.reset(), loop).result()

        _last_origin: typing.Optional[float] = None

        @player.property_observer("time-pos")
        def _time_observer(_name, value: typing.Optional[float]):
            nonlocal _last_origin

            if player["pause"] or value is None:
                return

            if _last_origin is None:
                _last_origin = time.time() - value

            if 0.1 < abs(time.time() - (_last_origin + value)):
                logger.info("Drift detected, resetting frames.")
                asyncio.run_coroutine_threadsafe(ossm_controller.reset(), loop).result()
                _last_origin = time.time() - value

            asyncio.run_coroutine_threadsafe(
                ossm_controller.send_frames_at(int((player.time_pos or value) * 1000)),
                loop,
            ).result()

        ossm_gui = OSSMGui(ossm_controller, loop)

        def _play():
            player.play(config.video)
            player.wait_for_playback()

        def _stop():
            ossm_gui.close()
            player.stop()

        loop.add_signal_handler(signal.SIGINT, _stop)
        loop.add_signal_handler(signal.SIGTERM, _stop)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            finished_tasks, pending_tasks = await asyncio.wait(
                [
                    loop.run_in_executor(executor, _play),
                    loop.run_in_executor(executor, ossm_gui.run),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
            _stop()
            for task in itertools.chain(finished_tasks, pending_tasks):
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        player.terminate()


@click.command()
@click.option(
    "-h",
    "--host",
    help="Host (and optional port) of the OSSM device. If not provided, MDNS will be used to discover the device on the local network.",
    default=None,
)
@click.option(
    "-s",
    "--script",
    help="Path to the script to load. If not provided, the name of the video file is used, replacing the extension with .funscript.",
    default=None,
)
@click.option("-m", "--min-depth", help="Minimum depth in mm.", default=0)
@click.option("-M", "--max-depth", help="Maximum depth in mm.", default=100)
@click.option("-d", "--delay-ms", help="Script delay in ms.", default=100)
@click.option(
    "-b", "--buffer-size", help="Maximum number of frames to preload.", default=1000
)
@click.option(
    "-p",
    "--preload-ms",
    help="Maximum number of milliseconds to preload.",
    default=30000,
)
@click.option(
    "-i",
    "--interpolator",
    help="Type of interpolation.",
    type=click.Choice(["linear", "hermite"]),
    default="linear",
)
@click.option(
    "-mh",
    "--mdns-hostname",
    help="MDSN hostname of the OSSM device.",
    default="ossm-stroke.local",
)
@click.option("-v", "--verbose", help="Enable verbose logging.", count=True)
@click.argument("video")
def main(
    script: typing.Optional[str],
    min_depth: int,
    max_depth: int,
    delay_ms: int,
    buffer_size: int,
    preload_ms: int,
    host: typing.Optional[str],
    interpolator: InterpolatorLiteral,
    mdns_hostname: str,
    verbose: int,
    video: str,
):
    """
    Play a video, syncing funscript file with an OSSM device.

    \tVIDEO:\t\tpath or URL to a video file.
    """
    log_level = logging.ERROR
    if 3 <= verbose:
        log_level = logging.DEBUG
    elif 2 <= verbose:
        log_level = logging.INFO
    elif 1 <= verbose:
        log_level = logging.WARNING
    logging.basicConfig(level=log_level)

    if script is None:
        if "." not in video:
            raise RuntimeError("You must specify a script path (--script).")

        script = video.rsplit(".", 1)[0] + ".funscript"

    if host is None:
        try:
            host = socket.gethostbyname(mdns_hostname)
        except socket.gaierror as exc:
            raise RuntimeError(
                "Couldn't find OSSM device on the local network. Please set --host option."
            ) from exc

    asyncio.run(
        play(
            Config(
                host=host,
                video=video,
                script_path=script,
                min_depth=min_depth,
                max_depth=max_depth,
                delay=delay_ms,
                buffer_size=buffer_size,
                preload_ms=preload_ms,
                interpolator=interpolator,
            )
        )
    )


if __name__ == "__main__":
    main(auto_envvar_prefix="OSSM_PLAYER")
