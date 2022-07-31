import asyncio
import bisect
import concurrent.futures
import dataclasses
import itertools
import json
import logging
import signal
import socket
import time
import tkinter
import tkinter.ttk
import typing

import click
import mpv
import websockets.client


logger = logging.getLogger("ossm-player")


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
    is_sent: bool = False


T = typing.TypeVar("T", int, float)


def scale(value: T, min_in: T, max_in: T, min_out: T, max_out: T) -> float:
    return ((max_out - min_out) * (value - min_in) / (max_in - min_in)) + min_out


class OSSMCommandList(list[OSSMCommand]):
    def __init__(self, funscript: Funscript, config: Config):
        for fs_action in funscript["actions"]:
            self.append(
                OSSMCommand(
                    depth=scale(
                        fs_action["pos"], 0, 100, config.min_depth, config.max_depth
                    ),
                    speed=0,
                    time=0,
                )
            )

        for i, command in enumerate(self[:-1]):
            current_depth = command.depth
            next_depth = self[i + 1].depth
            current_at = funscript["actions"][i]["at"]
            next_at = funscript["actions"][i + 1]["at"]

            command.depth = next_depth
            if next_at == current_at:
                command.speed = 0
            else:
                command.speed = abs(
                    (next_depth - current_depth) / ((next_at - current_at) / 1000.0)
                )
            command.time = current_at

        self.pop()

    def unsend_all(self):
        for action in self:
            action.is_sent = False

    def idx_by_timecode(self, timecode_ms: int) -> int:
        return bisect.bisect(self, timecode_ms, key=lambda command: command.time)


class OSSMController:
    def __init__(self, *, config: Config):
        self.config: Config = config

        self._mut: asyncio.Lock = asyncio.Lock()
        self._frames_producer: typing.Optional[asyncio.Task[None]] = None

        funscript: Funscript = load_funscript(config.script_path)
        self._commands = OSSMCommandList(funscript, config)

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
        await self._send_frame(self._commands[0].depth, 100, 0)

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

        self._commands.unsend_all()
        await self._clear_frames()

    async def send_frames_at(self, timecode_ms: int):
        if self._frames_producer:
            return
        self._frames_producer = asyncio.create_task(
            self._send_frames_at(timecode_ms, now=time.time()),
        )

    async def _send_frames_at(self, timecode_ms: int, now: float):
        timecode_ms += self.config.delay
        command_idx = self._commands.idx_by_timecode(timecode_ms)

        for command in self._commands[
            command_idx : command_idx + self.config.buffer_size
        ]:
            if command.is_sent:
                continue
            if self.config.preload_ms <= command.time - timecode_ms:
                break

            command_time = int(
                now * 1000 - self._beginning_of_time + command.time - timecode_ms
            )

            await self._send_frame(command.depth, command.speed, command_time)
            command.is_sent = True

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
                    await asyncio.wait_for(self._wait_for_ack(), timeout=1.0)
                except asyncio.TimeoutError as exc:
                    if max_retries <= n_retry:
                        raise exc
                    logger.exception("Didn't receive ACK in time. Retrying.")
                else:
                    break

    async def _send_frame(self, depth: float, speed: float, time: int):
        logger.info(f"sending frame {depth=} {speed=} {time=}")
        await self._send_and_wait_for_ack(
            {"type": "send_frame", "depth": depth, "speed": speed, "time": time},
        )

    async def _clear_frames(self):
        async with self._mut:
            await self._client.send(json.dumps({"type": "clear_frames"}))
            await self._wait_for_ack()


def load_funscript(path: str) -> Funscript:
    funscript: Funscript = json.loads(open(path).read())
    funscript["actions"] = sorted(funscript["actions"], key=lambda action: action["at"])
    if funscript["inverted"]:
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
                ossm_controller.send_frames_at(int(player.time_pos * 1000)),
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
            finished_task, pending_tasks = await asyncio.wait(
                [
                    loop.run_in_executor(executor, _play),
                    loop.run_in_executor(executor, ossm_gui.run),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
            _stop()
            for task in itertools.chain(finished_task, pending_tasks):
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
    "-b", "--buffer-size", help="Maximum number of frames to preload.", default=512
)
@click.option(
    "-p",
    "--preload-ms",
    help="Maximum number of milliseconds to preload.",
    default=60000,
)
@click.option(
    "-d",
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
            )
        )
    )


if __name__ == "__main__":
    main(auto_envvar_prefix="OSSM_PLAYER")
