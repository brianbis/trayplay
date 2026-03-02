"""
AirPlay System Tray Streamer — mirrors Windows system audio to an AirPlay (RAOP) receiver.

Key features (this version):
- LAN discovery (pyatv.scan) with a "Target Device" tray menu and manual add-by-IP.
- Persists settings + "most-used device" caching in %APPDATA%.
- Optional binding of all RAOP TCP/UDP sockets to a chosen local IPv4 (Auto/Manual/None)
  to work around multi-NIC routing (e.g. Tailscale route precedence).
- Audio source selection: All system audio (WASAPI loopback) or single-process capture
  (Windows Application Loopback API; Win10 build 20348+).
- Receiver volume control (pyatv Audio API) + local gain (preamp) fallback.
- Optional auto-start with Windows and auto-connect on launch.
- Optional auto-capture preferred app (defaults to Spotify) when it is producing audio.

Dependencies:
  pyatv pyaudiowpatch pycaw pystray Pillow
"""

from __future__ import annotations

# STA COM initialization — MUST be set before any comtypes/pycaw import so that
# MMNotificationClient callbacks dispatch on the main thread's message pump.
import sys
sys.coinit_flags = 0x2  # COINIT_APARTMENTTHREADED

import array
import asyncio
import ctypes
import ipaddress
import json
import logging
import os
import re
import socket
import struct
import subprocess
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple, cast as typing_cast

import comtypes
import winsound

# ──────────────────────────────────────────────────────────────────────────────
# Configuration + logging
# ──────────────────────────────────────────────────────────────────────────────

APP_NAME = "TrayPlay"
APP_VERSION = "0.1.0"
CONFIG_VERSION = 2

TARGET_SR = 44100
TARGET_CH = 2
DEFAULT_GAIN_DB = 0
GAIN_PRESETS = [-10, -5, 0, 5, 10, 15, 20, 25, 30, 35, 40]
RECONNECT_DELAY_DEFAULT = 10
SCAN_TIMEOUT_DEFAULT = 5
LIVE_BUFFER_TARGET_MS = 40
LIVE_BUFFER_MIN_MS = 35
LIVE_BUFFER_MAX_MS = 60
LIVE_BUFFER_STEP_MS = 5
LIVE_BUFFER_RETUNE_SEC = 5
SYSTEM_CAPTURE_TARGET_MS = 8
PROCESS_IDLE_POLL_SEC = 0.002
PROCESS_LIVENESS_CHECK_SEC = 0.5
RAOP_EXTRA_LATENCY_FRAMES = 11025
PROCESS_EVENT_WAIT_MS = 50

log = logging.getLogger("trayplay")
log.setLevel(logging.INFO)

try:
    import audioop  # type: ignore
except ImportError:
    audioop = None


def _appdata_dir() -> Path:
    root = os.getenv("APPDATA")
    if root:
        return Path(root) / APP_NAME
    return Path.home() / f".{APP_NAME.lower()}"

APP_DIR = _appdata_dir()
APP_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = APP_DIR / "config.json"
LOG_PATH = APP_DIR / "trayplay.log"

try:
    from logging.handlers import RotatingFileHandler

    _file_handler = RotatingFileHandler(
        LOG_PATH, maxBytes=2_000_000, backupCount=3, encoding="utf-8"
    )
    _file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S")
    )
    logging.getLogger().addHandler(_file_handler)
except Exception:
    pass

_console = logging.StreamHandler()
_console.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S")
)
logging.getLogger().addHandler(_console)
logging.getLogger().setLevel(logging.INFO)


@dataclass
class AppConfig:
    version: int = CONFIG_VERSION

    # Device selection / caching
    selected_device_id: Optional[str] = None  # BaseConfig.identifier
    selected_device_name: Optional[str] = None
    selected_device_address: Optional[str] = None  # for unicast scan fallback
    device_usage: Dict[str, int] = field(default_factory=dict)  # identifier -> count

    # Network binding (work-around for route precedence issues)
    bind_mode: str = "auto"  # "auto" | "manual" | "none"
    manual_local_ip: Optional[str] = None

    # Streaming + reconnection
    reconnect_delay_sec: int = RECONNECT_DELAY_DEFAULT
    auto_connect_on_launch: bool = False
    scan_timeout_sec: int = SCAN_TIMEOUT_DEFAULT

    # Audio processing
    gain_db: int = DEFAULT_GAIN_DB
    enable_limiter: bool = True  # hard limiter after gain

    # Receiver volume
    receiver_volume: Optional[float] = None  # 0..100 (best-effort)

    # Convenience / UX
    autostart_with_windows: bool = False
    auto_capture_preferred_app: bool = False
    preferred_app_names: List[str] = field(default_factory=lambda: ["spotify.exe"])
    hotkey_toggle: str = "ctrl+shift+p"


class ConfigStore:
    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()

    def load(self) -> AppConfig:
        with self._lock:
            if not self.path.exists():
                return AppConfig()
            try:
                raw = json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                log.exception("Failed to read config.json, starting with defaults")
                return AppConfig()

        cfg = AppConfig()
        if isinstance(raw, dict):
            cfg.version = int(raw.get("version", cfg.version) or cfg.version)

            cfg.selected_device_id = raw.get("selected_device_id") or None
            cfg.selected_device_name = raw.get("selected_device_name") or None
            cfg.selected_device_address = raw.get("selected_device_address") or None

            du = raw.get("device_usage")
            if isinstance(du, dict):
                cfg.device_usage = {str(k): int(v) for k, v in du.items() if str(k)}

            cfg.bind_mode = str(raw.get("bind_mode", cfg.bind_mode) or cfg.bind_mode)
            if cfg.bind_mode not in ("auto", "manual", "none"):
                cfg.bind_mode = "auto"
            cfg.manual_local_ip = raw.get("manual_local_ip") or None

            cfg.reconnect_delay_sec = int(
                raw.get("reconnect_delay_sec", cfg.reconnect_delay_sec)
                or cfg.reconnect_delay_sec
            )
            cfg.auto_connect_on_launch = bool(
                raw.get("auto_connect_on_launch", cfg.auto_connect_on_launch)
            )
            cfg.scan_timeout_sec = int(
                raw.get("scan_timeout_sec", cfg.scan_timeout_sec) or cfg.scan_timeout_sec
            )

            cfg.gain_db = int(raw.get("gain_db", cfg.gain_db) or cfg.gain_db)
            cfg.enable_limiter = bool(raw.get("enable_limiter", cfg.enable_limiter))

            rv = raw.get("receiver_volume")
            if isinstance(rv, (int, float)) and 0.0 <= float(rv) <= 100.0:
                cfg.receiver_volume = float(rv)

            cfg.autostart_with_windows = bool(
                raw.get("autostart_with_windows", cfg.autostart_with_windows)
            )
            cfg.auto_capture_preferred_app = bool(
                raw.get("auto_capture_preferred_app", cfg.auto_capture_preferred_app)
            )

            pan = raw.get("preferred_app_names")
            if isinstance(pan, list) and all(isinstance(x, str) for x in pan):
                cfg.preferred_app_names = [x.strip() for x in pan if x.strip()] or cfg.preferred_app_names

            ht = raw.get("hotkey_toggle")
            if isinstance(ht, str):
                cfg.hotkey_toggle = ht.strip()

        return cfg

    def save(self, cfg: AppConfig) -> None:
        with self._lock:
            payload = {
                "version": cfg.version,
                "selected_device_id": cfg.selected_device_id,
                "selected_device_name": cfg.selected_device_name,
                "selected_device_address": cfg.selected_device_address,
                "device_usage": cfg.device_usage,
                "bind_mode": cfg.bind_mode,
                "manual_local_ip": cfg.manual_local_ip,
                "reconnect_delay_sec": cfg.reconnect_delay_sec,
                "auto_connect_on_launch": cfg.auto_connect_on_launch,
                "scan_timeout_sec": cfg.scan_timeout_sec,
                "gain_db": cfg.gain_db,
                "enable_limiter": cfg.enable_limiter,
                "receiver_volume": cfg.receiver_volume,
                "autostart_with_windows": cfg.autostart_with_windows,
                "auto_capture_preferred_app": cfg.auto_capture_preferred_app,
                "preferred_app_names": cfg.preferred_app_names,
                "hotkey_toggle": cfg.hotkey_toggle,
            }
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            tmp.replace(self.path)


# ──────────────────────────────────────────────────────────────────────────────
# Windows autostart helper (HKCU Run)
# ──────────────────────────────────────────────────────────────────────────────

def set_autostart_enabled(enable: bool) -> None:
    try:
        import winreg  # type: ignore
    except Exception as e:
        raise OSError(f"winreg not available: {e}") from e

    run_key = r"Software\Microsoft\Windows\CurrentVersion\Run"
    name = APP_NAME

    if getattr(sys, "frozen", False):
        cmd = f'"{sys.executable}"'
    else:
        script_path = Path(__file__).resolve()
        cmd = f'"{sys.executable}" "{script_path}"'

    with winreg.OpenKey(winreg.HKEY_CURRENT_USER, run_key, 0, winreg.KEY_SET_VALUE) as key:
        if enable:
            winreg.SetValueEx(key, name, 0, winreg.REG_SZ, cmd)
        else:
            try:
                winreg.DeleteValue(key, name)
            except FileNotFoundError:
                pass


def is_autostart_enabled() -> bool:
    try:
        import winreg  # type: ignore
    except Exception:
        return False
    run_key = r"Software\Microsoft\Windows\CurrentVersion\Run"
    name = APP_NAME
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, run_key, 0, winreg.KEY_READ) as key:
            winreg.QueryValueEx(key, name)
            return True
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Process Loopback API (Windows Application Loopback)
# ──────────────────────────────────────────────────────────────────────────────

IID_IAudioClient = comtypes.GUID("{1CB9AD4C-DBFA-4c32-B178-C2F568A703B2}")
IID_IAudioCaptureClient = comtypes.GUID("{C8ADBD64-E71E-48a0-A4DE-185C395CD317}")
IID_IAgileObject = comtypes.GUID("{94EA2B94-E9CC-49E0-C0FF-EE64CA8F5B90}")
IID_IActivateAudioInterfaceAsyncOperation = comtypes.GUID(
    "{72A22D78-CDE4-431D-B8CC-843A71199B6D}"
)
VIRTUAL_AUDIO_DEVICE_PROCESS_LOOPBACK = "VAD\\Process_Loopback"

AUDCLNT_SHAREMODE_SHARED = 0
AUDCLNT_STREAMFLAGS_LOOPBACK = 0x00020000
AUDCLNT_STREAMFLAGS_EVENTCALLBACK = 0x00040000
AUDCLNT_STREAMFLAGS_AUTOCONVERTPCM = 0x80000000
AUDCLNT_STREAMFLAGS_SRC_DEFAULT_QUALITY = 0x08000000
AUDIOCLIENT_ACTIVATION_TYPE_PROCESS_LOOPBACK = 1
PROCESS_LOOPBACK_MODE_INCLUDE_TARGET_PROCESS_TREE = 0


class IAgileObject(comtypes.IUnknown):
    _iid_ = IID_IAgileObject
    _methods_ = []


class WAVEFORMATEX(ctypes.Structure):
    _fields_ = [
        ("wFormatTag", ctypes.c_ushort),
        ("nChannels", ctypes.c_ushort),
        ("nSamplesPerSec", ctypes.c_uint),
        ("nAvgBytesPerSec", ctypes.c_uint),
        ("nBlockAlign", ctypes.c_ushort),
        ("wBitsPerSample", ctypes.c_ushort),
        ("cbSize", ctypes.c_ushort),
    ]


class AUDIOCLIENT_PROCESS_LOOPBACK_PARAMS(ctypes.Structure):
    _fields_ = [("TargetProcessId", ctypes.c_uint), ("ProcessLoopbackMode", ctypes.c_uint)]


class AUDIOCLIENT_ACTIVATION_PARAMS(ctypes.Structure):
    _fields_ = [
        ("ActivationType", ctypes.c_uint),
        ("ProcessLoopbackParams", AUDIOCLIENT_PROCESS_LOOPBACK_PARAMS),
    ]


class PROPVARIANT_BLOB(ctypes.Structure):
    _fields_ = [
        ("vt", ctypes.c_ushort),
        ("pad1", ctypes.c_ushort),
        ("pad2", ctypes.c_ushort),
        ("pad3", ctypes.c_ushort),
        ("cbSize", ctypes.c_uint),
        ("pBlobData", ctypes.c_void_p),
    ]


class IAudioClient(comtypes.IUnknown):
    _iid_ = IID_IAudioClient
    _methods_ = [
        comtypes.COMMETHOD(
            [],
            ctypes.HRESULT,
            "Initialize",
            (["in"], ctypes.c_uint, "ShareMode"),
            (["in"], ctypes.c_uint, "StreamFlags"),
            (["in"], ctypes.c_longlong, "hnsBufferDuration"),
            (["in"], ctypes.c_longlong, "hnsPeriodicity"),
            (["in"], ctypes.POINTER(WAVEFORMATEX), "pFormat"),
            (["in"], ctypes.POINTER(comtypes.GUID), "AudioSessionGuid"),
        ),
        comtypes.COMMETHOD(
            [],
            ctypes.HRESULT,
            "GetBufferSize",
            (["out"], ctypes.POINTER(ctypes.c_uint), "pNumBufferFrames"),
        ),
        comtypes.COMMETHOD(
            [],
            ctypes.HRESULT,
            "GetStreamLatency",
            (["out"], ctypes.POINTER(ctypes.c_longlong), "phnsLatency"),
        ),
        comtypes.COMMETHOD(
            [],
            ctypes.HRESULT,
            "GetCurrentPadding",
            (["out"], ctypes.POINTER(ctypes.c_uint), "pNumPaddingFrames"),
        ),
        comtypes.COMMETHOD(
            [],
            ctypes.HRESULT,
            "IsFormatSupported",
            (["in"], ctypes.c_uint, "ShareMode"),
            (["in"], ctypes.POINTER(WAVEFORMATEX), "pFormat"),
            (["out"], ctypes.POINTER(ctypes.POINTER(WAVEFORMATEX)), "ppClosestMatch"),
        ),
        comtypes.COMMETHOD(
            [],
            ctypes.HRESULT,
            "GetMixFormat",
            (["out"], ctypes.POINTER(ctypes.POINTER(WAVEFORMATEX)), "ppDeviceFormat"),
        ),
        comtypes.COMMETHOD(
            [],
            ctypes.HRESULT,
            "GetDevicePeriod",
            (["out"], ctypes.POINTER(ctypes.c_longlong), "phnsDefaultDevicePeriod"),
            (["out"], ctypes.POINTER(ctypes.c_longlong), "phnsMinimumDevicePeriod"),
        ),
        comtypes.COMMETHOD([], ctypes.HRESULT, "Start"),
        comtypes.COMMETHOD([], ctypes.HRESULT, "Stop"),
        comtypes.COMMETHOD([], ctypes.HRESULT, "Reset"),
        comtypes.COMMETHOD(
            [],
            ctypes.HRESULT,
            "SetEventHandle",
            (["in"], ctypes.c_void_p, "eventHandle"),
        ),
        comtypes.COMMETHOD(
            [],
            ctypes.HRESULT,
            "GetService",
            (["in"], ctypes.POINTER(comtypes.GUID), "riid"),
            (["out"], ctypes.POINTER(ctypes.c_void_p), "ppv"),
        ),
    ]


class IAudioCaptureClient(comtypes.IUnknown):
    _iid_ = IID_IAudioCaptureClient
    _methods_ = [
        comtypes.COMMETHOD(
            [],
            ctypes.HRESULT,
            "GetBuffer",
            (["out"], ctypes.POINTER(ctypes.POINTER(ctypes.c_byte)), "ppData"),
            (["out"], ctypes.POINTER(ctypes.c_uint), "pNumFramesAvailable"),
            (["out"], ctypes.POINTER(ctypes.c_uint), "pdwFlags"),
            (["out"], ctypes.POINTER(ctypes.c_ulonglong), "pu64DevicePosition"),
            (["out"], ctypes.POINTER(ctypes.c_ulonglong), "pu64QPCPosition"),
        ),
        comtypes.COMMETHOD([], ctypes.HRESULT, "ReleaseBuffer", (["in"], ctypes.c_uint, "NumFramesRead")),
        comtypes.COMMETHOD([], ctypes.HRESULT, "GetNextPacketSize", (["out"], ctypes.POINTER(ctypes.c_uint), "pNumFramesInNextPacket")),
    ]


class IActivateAudioInterfaceAsyncOperation(comtypes.IUnknown):
    _iid_ = IID_IActivateAudioInterfaceAsyncOperation
    _methods_ = [
        comtypes.COMMETHOD(
            [],
            ctypes.HRESULT,
            "GetActivateResult",
            (["out"], ctypes.POINTER(ctypes.HRESULT), "activateResult"),
            (["out"], ctypes.POINTER(ctypes.POINTER(comtypes.IUnknown)), "activatedInterface"),
        ),
    ]


class IActivateAudioInterfaceCompletionHandler(comtypes.IUnknown):
    _iid_ = comtypes.GUID("{41D949AB-9862-444A-80F6-C261334DA5EB}")
    _methods_ = [
        comtypes.COMMETHOD(
            [],
            ctypes.HRESULT,
            "ActivateCompleted",
            (["in"], ctypes.POINTER(IActivateAudioInterfaceAsyncOperation), "activateOperation"),
        ),
    ]


class ActivationCompletionHandler(comtypes.COMObject):
    _com_interfaces_ = [IActivateAudioInterfaceCompletionHandler, IAgileObject]

    def __init__(self):
        super().__init__()
        self.completed = threading.Event()

    def ActivateCompleted(self, activateOperation):  # noqa: N802
        self.completed.set()
        return 0


def open_process_loopback(
    pid: int,
    sample_rate: int = TARGET_SR,
    channels: int = TARGET_CH,
    event_driven: bool = True,
):
    loopback_params = AUDIOCLIENT_PROCESS_LOOPBACK_PARAMS(
        TargetProcessId=pid,
        ProcessLoopbackMode=PROCESS_LOOPBACK_MODE_INCLUDE_TARGET_PROCESS_TREE,
    )
    act_params = AUDIOCLIENT_ACTIVATION_PARAMS(
        ActivationType=AUDIOCLIENT_ACTIVATION_TYPE_PROCESS_LOOPBACK,
        ProcessLoopbackParams=loopback_params,
    )

    propvar = PROPVARIANT_BLOB()
    propvar.vt = 0x41
    propvar.cbSize = ctypes.sizeof(act_params)
    propvar.pBlobData = ctypes.cast(ctypes.pointer(act_params), ctypes.c_void_p)

    handler = ActivationCompletionHandler()
    handler_unk = handler.QueryInterface(IActivateAudioInterfaceCompletionHandler)

    operation = ctypes.POINTER(IActivateAudioInterfaceAsyncOperation)()
    mmdevapi = ctypes.WinDLL("mmdevapi")

    hr = mmdevapi.ActivateAudioInterfaceAsync(
        ctypes.c_wchar_p(VIRTUAL_AUDIO_DEVICE_PROCESS_LOOPBACK),
        ctypes.byref(IID_IAudioClient),
        ctypes.byref(propvar),
        handler_unk,
        ctypes.byref(operation),
    )
    if hr != 0:
        raise OSError(f"ActivateAudioInterfaceAsync failed: 0x{hr & 0xFFFFFFFF:08X}")

    if not handler.completed.wait(timeout=5):
        raise TimeoutError("ActivateAudioInterfaceAsync timed out")

    if not operation:
        raise OSError("ActivateAudioInterfaceAsync returned null operation")

    activate_hr, activated_unk = operation.GetActivateResult()
    if int(activate_hr) != 0:
        raise OSError(f"Audio activation failed: 0x{int(activate_hr) & 0xFFFFFFFF:08X}")

    audio_client = activated_unk.QueryInterface(IAudioClient)

    fmt = WAVEFORMATEX()
    fmt.wFormatTag = 1
    fmt.nChannels = channels
    fmt.nSamplesPerSec = sample_rate
    fmt.wBitsPerSample = 16
    fmt.nBlockAlign = (fmt.nChannels * fmt.wBitsPerSample) // 8
    fmt.nAvgBytesPerSec = fmt.nSamplesPerSec * fmt.nBlockAlign
    fmt.cbSize = 0

    stream_flags = (
        AUDCLNT_STREAMFLAGS_LOOPBACK
        | AUDCLNT_STREAMFLAGS_AUTOCONVERTPCM
        | AUDCLNT_STREAMFLAGS_SRC_DEFAULT_QUALITY
    )
    if event_driven:
        stream_flags |= AUDCLNT_STREAMFLAGS_EVENTCALLBACK

    event_handle = None

    null_guid = ctypes.POINTER(comtypes.GUID)()
    try:
        hr = audio_client.Initialize(
            AUDCLNT_SHAREMODE_SHARED,
            stream_flags,
            0,
            0,
            ctypes.byref(fmt),
            null_guid,
        )
        if hr != 0:
            raise OSError(f"IAudioClient.Initialize failed: 0x{hr & 0xFFFFFFFF:08X}")

        if event_driven:
            kernel32 = ctypes.windll.kernel32
            event_handle = kernel32.CreateEventW(None, False, False, None)
            if not event_handle:
                raise OSError("CreateEventW failed")
            hr = audio_client.SetEventHandle(event_handle)
            if hr != 0:
                raise OSError(f"IAudioClient.SetEventHandle failed: 0x{hr & 0xFFFFFFFF:08X}")
    except Exception:
        if event_handle:
            ctypes.windll.kernel32.CloseHandle(event_handle)
        raise

    capture_ptr = audio_client.GetService(IID_IAudioCaptureClient)
    if not capture_ptr:
        if event_handle:
            ctypes.windll.kernel32.CloseHandle(event_handle)
        raise OSError("GetService(IAudioCaptureClient) returned null")
    capture_client = ctypes.cast(capture_ptr, ctypes.POINTER(IAudioCaptureClient))
    return audio_client, capture_client, fmt, event_handle


# ──────────────────────────────────────────────────────────────────────────────
# Dynamic LAN binding — patched into pyatv stack (optional)
# ──────────────────────────────────────────────────────────────────────────────

def list_local_ipv4_addresses() -> List[str]:
    ips: List[str] = []
    try:
        out = subprocess.check_output(["ipconfig"], text=True, errors="ignore")
        for ip in re.findall(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", out):
            if ip.startswith(("127.", "169.254.")):
                continue
            parts = ip.split(".")
            if any(not (0 <= int(p) <= 255) for p in parts):
                continue
            ips.append(ip)
    except Exception:
        pass

    try:
        hostname = socket.gethostname()
        for family, _, _, _, sockaddr in socket.getaddrinfo(hostname, None, family=socket.AF_INET):
            if family == socket.AF_INET:
                ip = sockaddr[0]
                if ip.startswith(("127.", "169.254.")):
                    continue
                ips.append(ip)
    except Exception:
        pass

    seen = set()
    uniq: List[str] = []
    for ip in ips:
        if ip not in seen:
            seen.add(ip)
            uniq.append(ip)
    return uniq


class NetworkBinder:
    def __init__(self, mode: str = "auto", manual_ip: Optional[str] = None):
        self._lock = threading.Lock()
        self.mode = mode
        self.manual_ip = manual_ip
        self.local_ip: Optional[str] = None

    def configure(self, mode: str, manual_ip: Optional[str]) -> None:
        with self._lock:
            self.mode = mode
            self.manual_ip = manual_ip
            self.local_ip = None

    def update_for_target(self, target_ip: str) -> Optional[str]:
        with self._lock:
            if self.mode == "none":
                self.local_ip = None
                return None
            if self.mode == "manual":
                self.local_ip = self.manual_ip
                return self.local_ip
            self.local_ip = self._auto_pick_local_ip(target_ip)
            return self.local_ip

    @staticmethod
    def _auto_pick_local_ip(target_ip: str) -> Optional[str]:
        chosen: Optional[str] = None
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                s.connect((target_ip, 9))
                ip = s.getsockname()[0]
                if ip and not ip.startswith(("127.", "169.254.")):
                    chosen = ip
            finally:
                s.close()
        except Exception:
            pass

        candidates = list_local_ipv4_addresses()
        try:
            target = ipaddress.IPv4Address(target_ip)
        except Exception:
            return chosen

        def is_cgnat(ip: str) -> bool:
            try:
                a = ipaddress.IPv4Address(ip)
                return ipaddress.IPv4Address("100.64.0.0") <= a <= ipaddress.IPv4Address("100.127.255.255")
            except Exception:
                return False

        if chosen and is_cgnat(chosen):
            for cand in candidates:
                try:
                    net = ipaddress.IPv4Network(f"{cand}/24", strict=False)
                    if target in net and ipaddress.IPv4Address(cand).is_private and not is_cgnat(cand):
                        return cand
                except Exception:
                    continue

        if chosen:
            return chosen

        for cand in candidates:
            try:
                a = ipaddress.IPv4Address(cand)
                if a.is_private and not is_cgnat(cand):
                    return cand
            except Exception:
                continue
        return None


_BINDER: Optional[NetworkBinder] = None


def _bound_local_addr() -> Optional[Tuple[str, int]]:
    if _BINDER and _BINDER.local_ip:
        return (_BINDER.local_ip, 0)
    return None


# Patch pyatv sockets to use our binder (optional)
import pyatv.support.http as _http

_orig_http_connect = _http.http_connect


async def _patched_http_connect(address, port):
    loop = asyncio.get_running_loop()
    local_addr = _bound_local_addr()
    kwargs = {}
    if local_addr:
        kwargs["local_addr"] = local_addr
    _, connection = await loop.create_connection(_http.HttpConnection, address, port, **kwargs)
    return typing_cast(_http.HttpConnection, connection)


_http.http_connect = _patched_http_connect

import pyatv.auth.hap_channel as _hap_channel
from pyatv.auth.hap_channel import AbstractHAPChannel


async def _patched_setup_channel(factory, verifier, address, port, salt, output_info, input_info):
    out_key, in_key = verifier.encryption_keys(salt, output_info, input_info)
    loop = asyncio.get_running_loop()
    local_addr = _bound_local_addr()
    kwargs = {}
    if local_addr:
        kwargs["local_addr"] = local_addr
    transport, protocol = await loop.create_connection(lambda: factory(out_key, in_key), address, port, **kwargs)
    return transport, typing_cast(AbstractHAPChannel, protocol)


_hap_channel.setup_channel = _patched_setup_channel

import pyatv.protocols.raop.protocols.airplayv2 as _av2

_av2.setup_channel = _patched_setup_channel

import pyatv.protocols.raop.stream_client as _sc

_orig_initialize = _sc.StreamClient.initialize


async def _patched_initialize(self, properties):
    local_addr = _bound_local_addr()
    if local_addr:
        self.rtsp.connection._local_ip = local_addr[0]
    return await _orig_initialize(self, properties)


_sc.StreamClient.initialize = _patched_initialize

_orig_send_audio = _sc.StreamClient.send_audio


async def _patched_send_audio(self, *args, **kwargs):
    local_addr = _bound_local_addr()
    if local_addr and not hasattr(self.loop, '_raop_bound'):
        _orig_create = self.loop.create_datagram_endpoint

        async def _bound_create(protocol_factory, remote_addr=None, **kw):
            if _bound_local_addr() and remote_addr and "local_addr" not in kw:
                kw["local_addr"] = _bound_local_addr()
            return await _orig_create(protocol_factory, remote_addr=remote_addr, **kw)

        self.loop.create_datagram_endpoint = _bound_create
        self.loop._raop_bound = True
    return await _orig_send_audio(self, *args, **kwargs)


_sc.StreamClient.send_audio = _patched_send_audio

import pyatv.protocols.raop as _raop

_raop.http_connect = _patched_http_connect

# Patch: Allow passing a custom AudioSource directly to stream_file
import pyatv.protocols.raop.audio_source as _audio_source

_orig_open_source = _audio_source.open_source


async def _patched_open_source(source, *args, **kwargs):
    if isinstance(source, _audio_source.AudioSource):
        return source
    return await _orig_open_source(source, *args, **kwargs)


_audio_source.open_source = _patched_open_source
_raop.open_source = _patched_open_source

# Core imports
import pyatv
from pyatv.const import Protocol
from pyatv.protocols.raop.parsers import get_audio_properties
import pyatv.protocols.raop.protocols as _raop_protocols
from pyatv.support.metadata import EMPTY_METADATA
import pyaudiowpatch as pyaudio


def _apply_low_latency_raop_context(ctx) -> None:
    base_latency = ctx.sample_rate + RAOP_EXTRA_LATENCY_FRAMES
    if ctx.latency > base_latency:
        ctx.latency = base_latency


_orig_stream_context_init = _raop_protocols.StreamContext.__init__
_orig_stream_context_reset = _raop_protocols.StreamContext.reset


def _patched_stream_context_init(self) -> None:
    _orig_stream_context_init(self)
    _apply_low_latency_raop_context(self)


def _patched_stream_context_reset(self) -> None:
    _orig_stream_context_reset(self)
    _apply_low_latency_raop_context(self)


_raop_protocols.StreamContext.__init__ = _patched_stream_context_init
_raop_protocols.StreamContext.reset = _patched_stream_context_reset

# pycaw imports (COM STA already set above)
from pycaw.pycaw import AudioUtilities
from pycaw.callbacks import MMNotificationClient

# System tray
import pystray
from PIL import Image, ImageDraw, ImageFont

# UI
import tkinter as tk
from tkinter import messagebox


# ──────────────────────────────────────────────────────────────────────────────
# LiveAudioSource
# ──────────────────────────────────────────────────────────────────────────────

class LiveAudioSource(_audio_source.AudioSource):
    def __init__(self, sample_rate: int, channels: int):
        self._sample_rate = sample_rate
        self._channels = channels
        self._sample_size = 2
        self._frame_size = channels * 2
        self._chunks = deque()
        self._chunk_offset = 0
        self._buffered_bytes = 0
        self._target_buffer_ms = 0
        self._max_buffer_bytes = 0
        self._dropped_bytes = 0
        self._peak_buffer_bytes = 0
        self._last_tune_dropped_bytes = 0
        self._stable_windows = 0
        self._next_stats_log = time.monotonic() + LIVE_BUFFER_RETUNE_SEC
        self._closed = False
        self._lock = threading.Lock()
        self._set_target_buffer_ms(LIVE_BUFFER_TARGET_MS)

    async def readframes(self, nframes):
        needed = nframes * self._frame_size
        pieces = bytearray()

        with self._lock:
            if self._closed and self._buffered_bytes == 0:
                return self.NO_FRAMES

            while needed > 0 and self._chunks:
                chunk = self._chunks[0]
                available = len(chunk) - self._chunk_offset
                take = min(needed, available)
                start = self._chunk_offset
                pieces.extend(chunk[start:start + take])
                needed -= take
                self._buffered_bytes -= take
                self._chunk_offset += take
                if self._chunk_offset >= len(chunk):
                    self._chunks.popleft()
                    self._chunk_offset = 0

        if needed > 0:
            pieces.extend(b"\x00" * needed)

        return bytes(pieces)

    def feed(self, pcm_data: bytes) -> None:
        if not pcm_data:
            return

        try:
            chunk = audioop.byteswap(pcm_data, 2)
        except Exception:
            samples = array.array("h")
            samples.frombytes(pcm_data)
            samples.byteswap()
            chunk = samples.tobytes()
        dropped = 0

        with self._lock:
            self._drop_oldest_locked(max(0, self._buffered_bytes + len(chunk) - self._max_buffer_bytes))
            dropped = self._dropped_bytes
            self._chunks.append(chunk)
            self._buffered_bytes += len(chunk)
            self._peak_buffer_bytes = max(self._peak_buffer_bytes, self._buffered_bytes)

            now = time.monotonic()
            if now >= self._next_stats_log:
                dropped_delta = self._dropped_bytes - self._last_tune_dropped_bytes
                self._retune_locked(dropped_delta)
                queued_ms = (self._buffered_bytes / (self._sample_rate * self._frame_size)) * 1000.0
                peak_ms = (self._peak_buffer_bytes / (self._sample_rate * self._frame_size)) * 1000.0
                log.debug(
                    "Live buffer: queued=%.1fms peak=%.1fms dropped=%.1fms target=%dms",
                    queued_ms,
                    peak_ms,
                    (dropped_delta / (self._sample_rate * self._frame_size)) * 1000.0,
                    self._target_buffer_ms,
                )
                self._last_tune_dropped_bytes = self._dropped_bytes
                self._peak_buffer_bytes = self._buffered_bytes
                self._next_stats_log = now + LIVE_BUFFER_RETUNE_SEC

    def _drop_oldest_locked(self, drop_bytes: int) -> None:
        if drop_bytes <= 0:
            return

        remainder = drop_bytes % self._frame_size
        if remainder:
            drop_bytes += self._frame_size - remainder

        to_drop = min(drop_bytes, self._buffered_bytes)
        while to_drop > 0 and self._chunks:
            chunk = self._chunks[0]
            available = len(chunk) - self._chunk_offset
            take = min(to_drop, available)
            self._chunk_offset += take
            self._buffered_bytes -= take
            self._dropped_bytes += take
            to_drop -= take
            if self._chunk_offset >= len(chunk):
                self._chunks.popleft()
                self._chunk_offset = 0

    def _bytes_for_ms(self, duration_ms: int) -> int:
        target = int(self._sample_rate * self._frame_size * duration_ms / 1000)
        target = max(self._frame_size, target)
        remainder = target % self._frame_size
        if remainder:
            target -= remainder
        return max(self._frame_size, target)

    def _set_target_buffer_ms(self, duration_ms: int) -> None:
        self._target_buffer_ms = max(LIVE_BUFFER_MIN_MS, min(LIVE_BUFFER_MAX_MS, int(duration_ms)))
        self._max_buffer_bytes = self._bytes_for_ms(self._target_buffer_ms)

    def _retune_locked(self, dropped_delta: int) -> None:
        grow_threshold = self._bytes_for_ms(8)
        shrink_threshold = self._bytes_for_ms(max(8, self._target_buffer_ms - LIVE_BUFFER_STEP_MS))

        if dropped_delta >= grow_threshold and self._target_buffer_ms < LIVE_BUFFER_MAX_MS:
            self._set_target_buffer_ms(self._target_buffer_ms + LIVE_BUFFER_STEP_MS)
            self._stable_windows = 0
        elif (
            dropped_delta == 0
            and self._peak_buffer_bytes <= shrink_threshold
            and self._target_buffer_ms > LIVE_BUFFER_MIN_MS
        ):
            self._stable_windows += 1
            if self._stable_windows >= 2:
                self._set_target_buffer_ms(self._target_buffer_ms - LIVE_BUFFER_STEP_MS)
                self._stable_windows = 0
        else:
            self._stable_windows = 0

    async def close(self) -> None:
        with self._lock:
            self._closed = True

    async def get_metadata(self):
        return EMPTY_METADATA

    @property
    def sample_rate(self) -> int:
        return self._sample_rate

    @property
    def channels(self) -> int:
        return self._channels

    @property
    def sample_size(self) -> int:
        return self._sample_size

    @property
    def duration(self) -> int:
        return 0


# ──────────────────────────────────────────────────────────────────────────────
# Audio utilities
# ──────────────────────────────────────────────────────────────────────────────

def apply_gain_limited(pcm_data: bytes, gain_db: int, limiter: bool = True) -> bytes:
    if gain_db == 0:
        return pcm_data
    factor = 10 ** (gain_db / 20.0)
    # audioop.mul wraps on overflow; only safe when factor <= 1.0 or limiter is off
    if audioop and (not limiter or factor <= 1.0):
        try:
            return audioop.mul(pcm_data, 2, factor)
        except Exception:
            pass
    samples = array.array("h")
    samples.frombytes(pcm_data)
    for i in range(len(samples)):
        v = int(samples[i] * factor)
        samples[i] = max(-32768, min(32767, v))
    return samples.tobytes()


def resample_linear(data: bytes, channels: int, ratio: float) -> bytes:
    samples = array.array("h")
    samples.frombytes(data)
    in_frames = len(samples) // channels
    out_frames = int(in_frames * ratio)
    if out_frames <= 0:
        return b""
    out = array.array("h", [0] * (out_frames * channels))
    for i in range(out_frames):
        src_pos = i / ratio
        idx = int(src_pos)
        frac = src_pos - idx
        if idx >= in_frames:
            idx = in_frames - 1
            frac = 0.0
        for c in range(channels):
            s0 = samples[idx * channels + c]
            s1 = samples[min(idx + 1, in_frames - 1) * channels + c]
            out[i * channels + c] = int(s0 + frac * (s1 - s0))
    return out.tobytes()


def resample_stream(
    data: bytes,
    channels: int,
    in_rate: int,
    out_rate: int,
    state=None,
):
    try:
        return audioop.ratecv(data, 2, channels, in_rate, out_rate, state)
    except Exception:
        ratio = out_rate / in_rate
        return resample_linear(data, channels, ratio), state


def downmix_to_stereo(data: bytes, in_channels: int) -> bytes:
    if in_channels <= 2:
        return data
    samples = array.array("h")
    samples.frombytes(data)
    in_frames = len(samples) // in_channels
    out = array.array("h", [0] * (in_frames * 2))
    for i in range(in_frames):
        out[i * 2] = samples[i * in_channels]
        out[i * 2 + 1] = samples[i * in_channels + 1]
    return out.tobytes()


# ──────────────────────────────────────────────────────────────────────────────
# Tray icon generation
# ──────────────────────────────────────────────────────────────────────────────

def _make_icon(color: str, letter: str = ""):
    img = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.ellipse([8, 8, 56, 56], fill=color)
    if letter:
        try:
            font = ImageFont.truetype("arial.ttf", 32)
        except Exception:
            font = ImageFont.load_default()
        bbox = draw.textbbox((0, 0), letter, font=font)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        draw.text(((64 - tw) / 2, (64 - th) / 2 - bbox[1]), letter, fill="white", font=font)
    return img


ICON_IDLE = _make_icon("#808080", "-")
ICON_STREAMING = _make_icon("#00CC00", ">")
ICON_ERROR = _make_icon("#CC0000", "!")


# ──────────────────────────────────────────────────────────────────────────────
# Device change listener (default output device)
# ──────────────────────────────────────────────────────────────────────────────

class DeviceChangeClient(MMNotificationClient):
    def __init__(self, restart_event: threading.Event):
        super().__init__()
        self._restart_event = restart_event

    def on_default_device_changed(self, flow, flow_id, role, role_id, default_device_id):
        if flow_id == 0 and role_id == 0:
            log.info("Default output device changed -> restarting capture")
            self._restart_event.set()


# ──────────────────────────────────────────────────────────────────────────────
# Device discovery model
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class DiscoveredDevice:
    identifier: str
    name: str
    address: str
    config: pyatv.conf.BaseConfig
    last_seen: float = field(default_factory=lambda: time.time())


# ──────────────────────────────────────────────────────────────────────────────
# Main application
# ──────────────────────────────────────────────────────────────────────────────

class AirPlayTray:
    def __init__(self):
        self._cfg_store = ConfigStore(CONFIG_PATH)
        self._cfg = self._cfg_store.load()

        # Sync autostart state (best effort)
        try:
            if self._cfg.autostart_with_windows != is_autostart_enabled():
                set_autostart_enabled(self._cfg.autostart_with_windows)
        except Exception as e:
            log.warning(f"Autostart sync failed: {e}")

        global _BINDER
        _BINDER = NetworkBinder(self._cfg.bind_mode, self._cfg.manual_local_ip)
        self._binder = _BINDER

        # High-level app events
        self._shutdown = threading.Event()        # app quit
        self._stop_requested = threading.Event()  # stop streaming requested by user

        # Capture control
        self._capture_stop = threading.Event()    # stops capture thread
        self._restart_capture = threading.Event()

        # State
        self._streaming = False
        self._status = "Idle"
        self._gain_db = int(self._cfg.gain_db)
        self._enable_limiter = bool(self._cfg.enable_limiter)

        self._async_loop: Optional[asyncio.AbstractEventLoop] = None
        self._async_thread: Optional[threading.Thread] = None
        self._capture_thread: Optional[threading.Thread] = None
        self._source: Optional[LiveAudioSource] = None
        self._icon: Optional[pystray.Icon] = None

        self._device_client: Optional[DeviceChangeClient] = None
        self._enumerator = None

        self._atv = None  # connected AppleTV (RAOP)

        # Audio source selection
        self._target_pid: Optional[int] = None
        self._target_name: Optional[str] = None
        self._stream_sample_rate = TARGET_SR
        self._stream_channels = TARGET_CH

        # Discovered devices
        self._devices_lock = threading.Lock()
        self._devices: Dict[str, DiscoveredDevice] = {}

        self._stream_future = None
        self._state_lock = threading.Lock()
        self._async_ready = threading.Event()
        self._ui_root = None
        self._ui_thread: Optional[threading.Thread] = None
        self._ui_ready = threading.Event()
        self._last_error: Optional[str] = None

    # ───────────────────────── Async loop helpers ─────────────────────────

    def _run_async_loop(self) -> None:
        self._async_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._async_loop)
        self._async_loop.call_soon(self._async_ready.set)
        self._async_loop.run_forever()

    def _schedule(self, coro):
        if not self._async_loop:
            raise RuntimeError("Async loop not ready")
        return asyncio.run_coroutine_threadsafe(coro, self._async_loop)

    # ───────────────────────── UI helpers ─────────────────────────────────

    def _run_ui_loop(self) -> None:
        root = tk.Tk()
        root.withdraw()
        self._ui_root = root
        self._ui_ready.set()
        try:
            root.mainloop()
        finally:
            self._ui_root = None
            self._ui_ready.clear()

    def _run_on_ui(self, callback: Callable[[], None]) -> None:
        if not self._ui_ready.wait(timeout=2):
            log.warning("UI thread not ready")
            return
        root = self._ui_root
        if root is None:
            log.warning("UI root not available")
            return
        def _wrapped():
            try:
                callback()
            except Exception:
                log.exception("Unhandled UI callback error")
        try:
            root.after(0, _wrapped)
        except Exception as e:
            log.warning(f"Failed to schedule UI work: {e}")

    def _close_dialog(self, win) -> None:
        try:
            win.grab_release()
        except Exception:
            pass
        try:
            win.destroy()
        except Exception:
            pass

    def _create_dialog(self, title: str, geometry: str, focus_widget=None):
        if self._ui_root is None:
            raise RuntimeError("UI root not ready")
        win = tk.Toplevel(self._ui_root)
        win.title(title)
        win.resizable(False, False)
        win.attributes("-topmost", True)
        # Position near mouse cursor
        try:
            mx, my = win.winfo_pointerxy()
            w, h = (int(x) for x in geometry.split("x"))
            sw, sh = win.winfo_screenwidth(), win.winfo_screenheight()
            x = max(0, min(mx - w // 2, sw - w))
            y = max(0, min(my - 20, sh - h))
            win.geometry(f"{geometry}+{x}+{y}")
        except Exception:
            win.geometry(geometry)
        win.protocol("WM_DELETE_WINDOW", lambda: self._close_dialog(win))
        win.bind("<Escape>", lambda e: self._close_dialog(win))
        win._focus_target = focus_widget

        def _activate(event=None):
            try:
                win.deiconify()
                win.lift()
                win.focus_force()
                target = win._focus_target
                if target:
                    target.focus_set()
                win.grab_set()
            except Exception:
                log.exception("Failed to activate dialog")

        win.bind("<Map>", _activate)
        return win

    def _show_error_dialog(self, title: str, message: str) -> None:
        def _show():
            if self._ui_root is None:
                return
            messagebox.showerror(title, message, parent=self._ui_root)

        self._run_on_ui(_show)

    def _show_info_dialog(self, title: str, message: str) -> None:
        def _show():
            if self._ui_root is None:
                return
            messagebox.showinfo(title, message, parent=self._ui_root)

        self._run_on_ui(_show)

    # ───────────────────────── Discovery / selection ──────────────────────

    async def _scan_devices_async(self, hosts: Optional[List[str]] = None) -> List[pyatv.conf.BaseConfig]:
        assert self._async_loop is not None
        proto = {Protocol.AirPlay, Protocol.RAOP}
        return await pyatv.scan(
            self._async_loop,
            timeout=self._cfg.scan_timeout_sec,
            protocol=proto,
            hosts=hosts,
        )

    def _scan_devices(self, hosts: Optional[List[str]] = None) -> None:
        try:
            fut = self._schedule(self._scan_devices_async(hosts=hosts))
        except Exception as e:
            log.warning(f"Scan schedule failed: {e}")
            return

        def _done(f):
            try:
                configs = f.result()
            except Exception as e:
                log.warning(f"Device scan failed: {e}")
                self._set_status(f"Scan failed: {e}", ICON_ERROR)
                self._show_error_dialog("Device Scan Failed", str(e))
                self._update_menu()
                return
            self._ingest_scan_results(configs)
            self._update_menu()

        fut.add_done_callback(_done)

    def _ingest_scan_results(self, configs: List[pyatv.conf.BaseConfig]) -> None:
        now = time.time()
        count = 0
        with self._devices_lock:
            for conf in configs:
                try:
                    if not conf.get_service(Protocol.RAOP):
                        continue
                    identifier = str(conf.identifier)
                    name = str(conf.name)
                    address = str(conf.address)
                    self._devices[identifier] = DiscoveredDevice(identifier, name, address, conf, now)
                    count += 1
                except Exception:
                    continue
        log.info(f"Scan complete: {count} RAOP device(s)")

    def _get_devices_sorted(self) -> List[DiscoveredDevice]:
        with self._devices_lock:
            devices = list(self._devices.values())
        usage = self._cfg.device_usage

        def key(d: DiscoveredDevice):
            return (int(usage.get(d.identifier, 0)), d.last_seen, d.name.lower())

        devices.sort(key=key, reverse=True)
        return devices

    def _select_device(self, identifier: str):
        def handler(icon=None, item=None):
            with self._devices_lock:
                dev = self._devices.get(identifier)
            if not dev:
                self._set_status("Device not found (rescan)", ICON_ERROR)
                self._update_menu()
                return

            self._cfg.selected_device_id = dev.identifier
            self._cfg.selected_device_name = dev.name
            self._cfg.selected_device_address = dev.address
            self._cfg.device_usage[dev.identifier] = int(self._cfg.device_usage.get(dev.identifier, 0)) + 1
            self._cfg_store.save(self._cfg)

            self._binder.update_for_target(dev.address)
            log.info(f"Selected device: {dev.name} ({dev.address})")

            if self._streaming:
                self._stop_streaming()
                time.sleep(0.2)
                self._start_streaming()

            self._update_menu()
        return handler

    def _pick_default_device_if_needed(self) -> Optional[DiscoveredDevice]:
        if self._cfg.selected_device_id:
            with self._devices_lock:
                dev = self._devices.get(self._cfg.selected_device_id)
            if dev:
                return dev

        devices = self._get_devices_sorted()
        if not devices:
            return None

        dev = devices[0]
        self._cfg.selected_device_id = dev.identifier
        self._cfg.selected_device_name = dev.name
        self._cfg.selected_device_address = dev.address
        self._cfg.device_usage[dev.identifier] = int(self._cfg.device_usage.get(dev.identifier, 0)) + 1
        self._cfg_store.save(self._cfg)
        return dev

    def _open_selection_dialog(
        self,
        title: str,
        prompt: str,
        items: List[Tuple[str, Callable[[], None]]],
        current_index: int = 0,
    ) -> None:
        if not items:
            self._show_info_dialog(title, "No items are currently available.")
            return

        def _show():
            win = self._create_dialog(title, "460x360")
            lf = tk.LabelFrame(win, text=prompt)
            lf.pack(padx=12, pady=(12, 8), fill=tk.BOTH, expand=True)
            listbox = tk.Listbox(lf, height=12, exportselection=False)
            for label, _ in items:
                listbox.insert(tk.END, label)
            listbox.pack(padx=4, pady=4, fill=tk.BOTH, expand=True)

            index = max(0, min(current_index, len(items) - 1))
            listbox.selection_clear(0, tk.END)
            listbox.selection_set(index)
            listbox.see(index)
            win._focus_target = listbox

            def on_apply():
                sel = listbox.curselection()
                if not sel:
                    messagebox.showerror("No selection", "Please select an item.", parent=win)
                    return
                _, handler = items[int(sel[0])]
                self._close_dialog(win)
                threading.Thread(target=handler, daemon=True, name="ui-selection-action").start()

            listbox.bind("<Double-Button-1>", lambda e: on_apply())
            listbox.bind("<Return>", lambda e: on_apply())

            btn_frame = tk.Frame(win, takefocus=0)
            btn_frame.pack(pady=(0, 12))
            tk.Button(btn_frame, text="Apply", command=on_apply, width=16).pack(side=tk.LEFT, padx=4)
            tk.Button(
                btn_frame,
                text="Cancel",
                command=lambda: self._close_dialog(win),
                width=16,
            ).pack(side=tk.LEFT, padx=4)

        self._run_on_ui(_show)

    def _open_device_picker_dialog(self, icon=None, item=None) -> None:
        devices = self._get_devices_sorted()
        items = [
            (f"{dev.name} ({dev.address})", self._select_device(dev.identifier))
            for dev in devices
        ]
        current_index = 0
        for idx, dev in enumerate(devices):
            if dev.identifier == self._cfg.selected_device_id:
                current_index = idx
                break
        self._open_selection_dialog(
            "Choose Target Device",
            "Select the AirPlay receiver to use.",
            items,
            current_index,
        )

    def _open_audio_source_picker_dialog(self, icon=None, item=None) -> None:
        sessions = self._get_audio_sessions()
        items = [("All Audio", self._set_audio_source_all)]
        items.extend(
            (f"{name} ({pid})", self._set_audio_source_pid(pid, name))
            for pid, name in sessions
        )
        current_index = 0
        if self._target_pid is not None:
            for idx, (pid, _) in enumerate(sessions, start=1):
                if pid == self._target_pid:
                    current_index = idx
                    break
        self._open_selection_dialog(
            "Choose Audio Source",
            "Select which audio source should be streamed.",
            items,
            current_index,
        )

    def _manual_add_device_dialog(self, icon=None, item=None) -> None:
        def _show():
            win = self._create_dialog("Add AirPlay Device", "380x180")

            lf = tk.LabelFrame(win, text="Device IP address (unicast scan)")
            lf.pack(padx=12, pady=(12, 0))
            entry = tk.Entry(lf, width=28)
            entry.pack(padx=4, pady=4)
            entry.insert(0, self._cfg.selected_device_address or "")
            win._focus_target = entry

            def on_add():
                ip = entry.get().strip()
                try:
                    ipaddress.ip_address(ip)
                except Exception:
                    messagebox.showerror("Invalid IP", "Please enter a valid IP address.", parent=win)
                    entry.focus_set()
                    return
                self._scan_devices(hosts=[ip])
                self._close_dialog(win)

            entry.bind("<Return>", lambda e: on_add())
            btn_frame = tk.Frame(win, takefocus=0)
            btn_frame.pack(pady=(12, 0))
            tk.Button(btn_frame, text="Scan & Add", command=on_add, width=16).pack(side=tk.LEFT, padx=4)
            tk.Button(
                btn_frame,
                text="Cancel",
                command=lambda: self._close_dialog(win),
                width=16,
            ).pack(side=tk.LEFT, padx=4)

        self._run_on_ui(_show)

    # ───────────────────────── Network binding UI ─────────────────────────

    def _set_bind_mode(self, mode: str):
        def handler(icon=None, item=None):
            if mode not in ("auto", "manual", "none"):
                return
            if mode == "manual":
                self._manual_bind_ip_dialog()
                return
            self._cfg.bind_mode = mode
            if mode == "none":
                self._cfg.manual_local_ip = None
            self._binder.configure(self._cfg.bind_mode, self._cfg.manual_local_ip)
            self._cfg_store.save(self._cfg)
            log.info(f"Bind mode set to: {mode}")
            self._update_menu()
        return handler

    def _manual_bind_ip_dialog(self) -> None:
        ips = list_local_ipv4_addresses()
        if not ips:
            self._set_status("No local IPv4 addresses found", ICON_ERROR)
            self._show_error_dialog("No Local IP Addresses", "No usable local IPv4 addresses were found.")
            self._update_menu()
            return

        def _show():
            win = self._create_dialog("Bind Local IP", "380x250")

            lf = tk.LabelFrame(win, text="Select local IPv4 to bind RAOP sockets")
            lf.pack(padx=12, pady=(12, 6), fill=tk.BOTH, expand=False)
            listbox = tk.Listbox(lf, height=7, exportselection=False)
            for ip in ips:
                listbox.insert(tk.END, ip)
            listbox.pack(padx=4, pady=4, fill=tk.BOTH, expand=False)

            if self._cfg.manual_local_ip and self._cfg.manual_local_ip in ips:
                idx = ips.index(self._cfg.manual_local_ip)
                listbox.selection_set(idx)
                listbox.see(idx)
            else:
                listbox.selection_set(0)
            win._focus_target = listbox

            def on_apply():
                sel = listbox.curselection()
                if not sel:
                    messagebox.showerror("No selection", "Please select an IP.", parent=win)
                    listbox.focus_set()
                    return
                ip = ips[int(sel[0])]
                self._cfg.bind_mode = "manual"
                self._cfg.manual_local_ip = ip
                self._binder.configure("manual", ip)
                self._cfg_store.save(self._cfg)
                log.info(f"Bind local IP set to: {ip}")
                self._close_dialog(win)
                self._update_menu()

            listbox.bind("<Double-Button-1>", lambda e: on_apply())
            listbox.bind("<Return>", lambda e: on_apply())
            btn_frame = tk.Frame(win, takefocus=0)
            btn_frame.pack(pady=(12, 0))
            tk.Button(btn_frame, text="Apply", command=on_apply, width=16).pack(side=tk.LEFT, padx=4)
            tk.Button(
                btn_frame,
                text="Cancel",
                command=lambda: self._close_dialog(win),
                width=16,
            ).pack(side=tk.LEFT, padx=4)

        self._run_on_ui(_show)

    # ───────────────────────── Capture thread ─────────────────────────────

    def _capture_loop(self) -> None:
        while not self._capture_stop.is_set():
            if self._cfg.auto_capture_preferred_app and self._target_pid is None:
                self._select_preferred_app_if_available()

            if self._target_pid is not None:
                self._capture_loop_process()
            else:
                self._capture_loop_system()

            if self._restart_capture.is_set() and not self._capture_stop.is_set():
                log.info("Restarting capture...")
                time.sleep(0.25)

    def _capture_loop_system(self) -> None:
        pa = None
        stream = None
        try:
            pa = pyaudio.PyAudio()
            loopback = pa.get_default_wasapi_loopback()
            native_sr = int(loopback["defaultSampleRate"])
            ch = max(int(loopback["maxInputChannels"]), 2)
            target_sr = int(self._stream_sample_rate or TARGET_SR)
            needs_downmix = ch > TARGET_CH
            capture_sr = target_sr
            chunk_frames = max(256, min(384, int(capture_sr * SYSTEM_CAPTURE_TARGET_MS / 1000)))
            resample_state = None

            log.info(
                f"Capturing (system): {loopback['name']} (device_sr={native_sr}, target_sr={target_sr}, ch={ch})"
            )
            try:
                stream = pa.open(
                    format=pyaudio.paInt16,
                    channels=ch,
                    rate=capture_sr,
                    input=True,
                    input_device_index=loopback["index"],
                    frames_per_buffer=chunk_frames,
                )
            except Exception:
                capture_sr = native_sr
                chunk_frames = max(256, min(384, int(capture_sr * SYSTEM_CAPTURE_TARGET_MS / 1000)))
                stream = pa.open(
                    format=pyaudio.paInt16,
                    channels=ch,
                    rate=capture_sr,
                    input=True,
                    input_device_index=loopback["index"],
                    frames_per_buffer=chunk_frames,
                )

            ratio = target_sr / capture_sr if capture_sr != target_sr else None

            self._restart_capture.clear()

            while not self._capture_stop.is_set() and not self._restart_capture.is_set():
                try:
                    data = stream.read(chunk_frames, exception_on_overflow=False)
                except OSError:
                    log.warning("Capture read error, restarting")
                    break

                if self._gain_db != 0:
                    data = apply_gain_limited(data, self._gain_db, limiter=self._enable_limiter)
                if needs_downmix:
                    data = downmix_to_stereo(data, ch)
                if ratio:
                    data, resample_state = resample_stream(
                        data,
                        TARGET_CH,
                        capture_sr,
                        target_sr,
                        resample_state,
                    )
                source = self._source
                if source:
                    source.feed(data)

        except Exception as e:
            log.error(f"Capture setup error: {e}")
            if not self._capture_stop.is_set():
                time.sleep(1)
        finally:
            if stream:
                try:
                    stream.stop_stream()
                    stream.close()
                except Exception:
                    pass
            if pa:
                try:
                    pa.terminate()
                except Exception:
                    pass

    def _capture_loop_process(self) -> None:
        audio_client = None
        event_handle = None
        try:
            comtypes.CoInitializeEx(comtypes.COINIT_MULTITHREADED)

            pid = int(self._target_pid or 0)
            log.info(f"Capturing (process): PID {pid} ({self._target_name})")

            target_sr = int(self._stream_sample_rate or TARGET_SR)
            target_ch = int(self._stream_channels or TARGET_CH)
            audio_client, capture_client, fmt, event_handle = open_process_loopback(
                pid,
                sample_rate=target_sr,
                channels=target_ch,
                event_driven=True,
            )

            sr = int(fmt.nSamplesPerSec)
            ch = int(fmt.nChannels)
            is_float = (fmt.wFormatTag == 3) or (fmt.wFormatTag == 0xFFFE and fmt.wBitsPerSample == 32)
            bytes_per_sample = int(fmt.wBitsPerSample // 8)
            frame_size = ch * bytes_per_sample

            ratio = (target_sr / sr) if sr != target_sr else None
            needs_downmix = ch > TARGET_CH
            resample_state = None

            audio_client.Start()
            self._restart_capture.clear()

            AUDCLNT_S_BUFFER_EMPTY = 0x08890001
            AUDCLNT_BUFFERFLAGS_SILENT = 0x2
            WAIT_OBJECT_0 = 0x00000000
            WAIT_TIMEOUT = 0x00000102
            next_process_check = 0.0

            while not self._capture_stop.is_set() and not self._restart_capture.is_set():
                # Check if target process still exists
                now = time.monotonic()
                if now >= next_process_check:
                    next_process_check = now + PROCESS_LIVENESS_CHECK_SEC
                    try:
                        import ctypes as _ct
                        h = _ct.windll.kernel32.OpenProcess(0x1000, False, pid)
                        if h:
                            _ct.windll.kernel32.CloseHandle(h)
                        else:
                            log.warning("Target process exited; switching to All Audio")
                            self._target_pid = None
                            self._target_name = None
                            self._restart_capture.set()
                            break
                    except Exception:
                        pass

                if event_handle:
                    wait_res = ctypes.windll.kernel32.WaitForSingleObject(event_handle, PROCESS_EVENT_WAIT_MS)
                    if wait_res == WAIT_TIMEOUT:
                        continue
                    if wait_res != WAIT_OBJECT_0:
                        raise OSError(f"WaitForSingleObject failed: 0x{int(wait_res) & 0xFFFFFFFF:08X}")

                while not self._capture_stop.is_set() and not self._restart_capture.is_set():
                    try:
                        packet_size = int(capture_client.GetNextPacketSize())
                    except comtypes.COMError as e:
                        hr = e.hresult & 0xFFFFFFFF
                        log.warning(f"GetNextPacketSize failed: 0x{hr:08X}")
                        break

                    if packet_size == 0:
                        break

                    n = 0
                    try:
                        data_ptr, num_frames, flags, _, _ = capture_client.GetBuffer()
                    except comtypes.COMError as e:
                        hr = e.hresult & 0xFFFFFFFF
                        if hr == AUDCLNT_S_BUFFER_EMPTY:
                            break
                        log.warning(f"GetBuffer failed: 0x{hr:08X}")
                        break

                    n = int(num_frames)
                    flags_i = int(flags)

                    try:
                        if n > 0:
                            byte_count = n * frame_size

                            if flags_i & AUDCLNT_BUFFERFLAGS_SILENT:
                                raw = b"\x00" * byte_count
                            else:
                                raw = ctypes.string_at(data_ptr, byte_count)

                            # Convert float32 -> int16 if needed (skip work if silent)
                            if is_float and not (flags_i & AUDCLNT_BUFFERFLAGS_SILENT):
                                float_arr = array.array('f')
                                float_arr.frombytes(raw)
                                int_samples = array.array('h', (max(-32768, min(32767, int(s * 32767))) for s in float_arr))
                                data = int_samples.tobytes()
                            else:
                                data = raw

                            data = apply_gain_limited(data, self._gain_db, limiter=self._enable_limiter)
                            if needs_downmix:
                                data = downmix_to_stereo(data, ch)
                            if ratio:
                                data, resample_state = resample_stream(
                                    data,
                                    TARGET_CH,
                                    sr,
                                    target_sr,
                                    resample_state,
                                )
                            source = self._source
                            if source:
                                source.feed(data)
                    finally:
                        capture_client.ReleaseBuffer(n)

        except Exception:
            log.exception("Process capture error")
            if not self._capture_stop.is_set():
                time.sleep(1)
        finally:
            if audio_client:
                try:
                    audio_client.Stop()
                except Exception:
                    pass
            if event_handle:
                try:
                    ctypes.windll.kernel32.CloseHandle(event_handle)
                except Exception:
                    pass
            try:
                comtypes.CoUninitialize()
            except Exception:
                pass

    # ───────────────────────── Streaming lifecycle ─────────────────────────

    def _stream_format_for(self, conf: pyatv.conf.BaseConfig) -> Tuple[int, int]:
        try:
            service = conf.get_service(Protocol.RAOP)
            if service:
                sample_rate, channels, sample_size = get_audio_properties(service.properties)
                if sample_size == 2 and channels >= 2:
                    return int(sample_rate), TARGET_CH
        except Exception as e:
            log.debug(f"Could not parse RAOP stream format: {e}")
        return TARGET_SR, TARGET_CH

    async def _get_target_config(self) -> Optional[pyatv.conf.BaseConfig]:
        if self._cfg.selected_device_id:
            with self._devices_lock:
                dev = self._devices.get(self._cfg.selected_device_id)
            if dev:
                return dev.config

        # multicast scan
        try:
            configs = await self._scan_devices_async()
            self._ingest_scan_results(configs)
        except Exception as e:
            log.warning(f"Scan while connecting failed: {e}")

        dev = self._pick_default_device_if_needed()
        if dev:
            return dev.config

        # unicast scan last address (best effort)
        if self._cfg.selected_device_address:
            try:
                configs = await self._scan_devices_async(hosts=[self._cfg.selected_device_address])
                self._ingest_scan_results(configs)
                if configs:
                    return configs[0]
            except Exception as e:
                log.warning(f"Unicast scan fallback failed: {e}")

        return None

    async def _stream_loop(self) -> None:
        while not self._shutdown.is_set() and not self._stop_requested.is_set():
            self._set_status("Connecting...", ICON_IDLE)

            conf = await self._get_target_config()
            if not conf:
                self._set_status("No AirPlay devices found (Rescan)", ICON_ERROR)
                self._streaming = False
                self._update_menu()
                return

            self._stream_sample_rate, self._stream_channels = self._stream_format_for(conf)
            log.info(
                f"RAOP stream format: sr={self._stream_sample_rate}, ch={self._stream_channels}, latency={RAOP_EXTRA_LATENCY_FRAMES}+1s"
            )

            target_ip = str(conf.address)
            self._binder.update_for_target(target_ip)
            if self._binder.local_ip:
                log.info(f"Binding RAOP sockets to local IP: {self._binder.local_ip}")

            atv = None
            try:
                log.info(f"Connecting to {conf.name} @ {conf.address}...")
                atv = await pyatv.connect(conf, self._async_loop, protocol=Protocol.RAOP)
                self._atv = atv
                log.info("Connected (RAOP)")
            except Exception as e:
                log.error(f"Connection failed: {e}")
                self._set_status(f"Error: {e}", ICON_ERROR)
                self._atv = None
                if self._shutdown.is_set() or self._stop_requested.is_set():
                    return
                await asyncio.sleep(max(1, int(self._cfg.reconnect_delay_sec)))
                continue

            try:
                self._source = LiveAudioSource(self._stream_sample_rate, self._stream_channels)

                self._capture_stop.clear()
                self._restart_capture.clear()
                self._capture_thread = threading.Thread(target=self._capture_loop, daemon=True, name="capture")
                self._capture_thread.start()

                label = self._target_name or "All Audio"
                self._set_status(f"Streaming ({label}) -> {conf.name}", ICON_STREAMING)
                with self._state_lock:
                    self._streaming = True
                self._update_menu()

                # Sync receiver volume: read actual, then apply configured
                try:
                    actual_vol = await atv.audio.volume
                    log.info(f"Receiver actual volume: {actual_vol:.1f}%")
                    if self._cfg.receiver_volume is None:
                        self._cfg.receiver_volume = float(actual_vol)
                        self._config_store.save(self._cfg)
                        self._update_menu()
                except Exception:
                    log.debug("Could not read receiver volume", exc_info=True)
                if self._cfg.receiver_volume is not None:
                    try:
                        await atv.audio.set_volume(float(self._cfg.receiver_volume))
                    except Exception:
                        pass

                await atv.stream.stream_file(self._source)

            except asyncio.CancelledError:
                return
            except Exception as e:
                log.error(f"Stream error: {e}")
                self._set_status(f"Error: {e}", ICON_ERROR)
            finally:
                self._streaming = False
                self._capture_stop.set()

                if self._source:
                    try:
                        await self._source.close()
                    except Exception:
                        pass
                    self._source = None

                if atv:
                    try:
                        atv.close()
                    except Exception:
                        pass
                self._atv = None

                if self._capture_thread and self._capture_thread.is_alive():
                    self._capture_thread.join(timeout=3)
                self._capture_thread = None

            if self._shutdown.is_set() or self._stop_requested.is_set():
                self._set_status("Idle", ICON_IDLE)
                self._update_menu()
                return

            self._set_status("Reconnecting...", ICON_ERROR)
            log.info(f"Reconnecting in {self._cfg.reconnect_delay_sec}s...")
            await asyncio.sleep(max(1, int(self._cfg.reconnect_delay_sec)))

        self._set_status("Idle", ICON_IDLE)
        self._update_menu()

    # ───────────────────────── Audio source selection ──────────────────────

    def _get_audio_sessions(self) -> List[Tuple[int, str]]:
        sessions: List[Tuple[int, str]] = []
        try:
            comtypes.CoInitializeEx(0x0)
        except OSError:
            pass
        try:
            for session in AudioUtilities.GetAllSessions():
                if session.Process:
                    try:
                        pid = int(session.Process.pid)
                        name = str(session.Process.name())
                        sessions.append((pid, name))
                    except Exception:
                        continue
        except Exception as e:
            log.warning(f"Could not enumerate audio sessions: {e}")
        finally:
            try:
                comtypes.CoUninitialize()
            except Exception:
                pass

        seen = set()
        uniq = []
        for pid, name in sessions:
            if pid not in seen:
                seen.add(pid)
                uniq.append((pid, name))
        uniq.sort(key=lambda x: (x[1].lower(), x[0]))
        return uniq

    def _select_preferred_app_if_available(self) -> None:
        prefs = {n.lower() for n in (self._cfg.preferred_app_names or [])}
        if not prefs:
            return
        for pid, name in self._get_audio_sessions():
            if name.lower() in prefs:
                if self._target_pid != pid:
                    self._target_pid = pid
                    self._target_name = name
                    log.info(f"Auto-selected preferred app: {name} (PID {pid})")
                    if self._streaming:
                        self._restart_capture.set()
                        self._update_menu()
                return

    def _update_source_status(self) -> None:
        if self._streaming and self._cfg.selected_device_name:
            label = self._target_name or "All Audio"
            self._status = f"Streaming ({label}) -> {self._cfg.selected_device_name}"
            if self._icon:
                try:
                    self._icon.title = f"AirPlay: {self._status}"
                except Exception:
                    pass

    def _set_audio_source_all(self, icon=None, item=None) -> None:
        if self._target_pid is None:
            return
        self._target_pid = None
        self._target_name = None
        log.info("Audio source: All Audio")
        if self._streaming:
            self._restart_capture.set()
        self._update_source_status()
        self._update_menu()

    def _set_audio_source_pid(self, pid: int, name: str):
        def handler(icon=None, item=None):
            self._target_pid = int(pid)
            self._target_name = str(name)
            log.info(f"Audio source: {name} (PID {pid})")
            if self._streaming:
                self._restart_capture.set()
            self._update_source_status()
            self._update_menu()
        return handler

    # ───────────────────────── Gain & receiver volume ──────────────────────

    def _set_gain(self, db: int):
        def handler(icon=None, item=None):
            self._gain_db = int(db)
            self._cfg.gain_db = self._gain_db
            self._cfg_store.save(self._cfg)
            log.info(f"Gain set to {db:+} dB")
            self._update_menu()
        return handler

    def _toggle_limiter(self, icon=None, item=None):
        self._enable_limiter = not self._enable_limiter
        self._cfg.enable_limiter = self._enable_limiter
        self._cfg_store.save(self._cfg)
        log.info(f"Limiter {'enabled' if self._enable_limiter else 'disabled'}")
        self._update_menu()

    def _open_gain_slider(self, icon=None, item=None):
        def _show():
            win = self._create_dialog("Local Gain (preamp)", "400x250")

            var = tk.IntVar(value=int(self._gain_db))

            def on_slide(val):
                db = int(float(val))
                self._gain_db = db

            lf = tk.LabelFrame(win, text="Local Gain (dB)")
            lf.pack(padx=12, pady=(10, 0), fill=tk.X)
            slider = tk.Scale(
                lf,
                from_=-20,
                to=40,
                orient=tk.HORIZONTAL,
                variable=var,
                command=on_slide,
                showvalue=True,
                length=300,
                resolution=1,
                label="Use Left and Right arrow keys to adjust",
            )
            slider.pack(padx=4, pady=4)
            win._focus_target = slider

            def on_apply():
                self._cfg.gain_db = int(self._gain_db)
                self._cfg_store.save(self._cfg)
                self._update_menu()
                self._close_dialog(win)

            slider.bind("<Return>", lambda e: on_apply())
            btn_frame = tk.Frame(win, takefocus=0)
            btn_frame.pack(pady=(8, 0))
            tk.Button(btn_frame, text="Apply", command=on_apply, width=16).pack(side=tk.LEFT, padx=4)
            tk.Button(
                btn_frame,
                text="Cancel",
                command=lambda: self._close_dialog(win),
                width=16,
            ).pack(side=tk.LEFT, padx=4)

        self._run_on_ui(_show)

    def _open_receiver_volume_popup(self, icon=None, item=None):
        def _show():
            current = float(self._cfg.receiver_volume if self._cfg.receiver_volume is not None else 50.0)

            win = tk.Toplevel(self._ui_root)
            win.title("Receiver Volume")
            win.attributes("-topmost", True)
            win.wm_attributes("-toolwindow", True)
            win.resizable(False, False)

            # Slider dimensions
            slider_len = 200
            win_w, win_h = 52, slider_len + 50

            # Position above the mouse pointer (near tray icon)
            mx, my = win.winfo_pointerxy()
            sw, sh = win.winfo_screenwidth(), win.winfo_screenheight()
            x = max(0, min(mx - win_w // 2, sw - win_w))
            y = max(0, min(my - win_h - 8, sh - win_h))
            win.geometry(f"{win_w}x{win_h}+{x}+{y}")

            win.configure(bg="#2b2b2b")

            pct_var = tk.StringVar(value=f"{int(current)}%")
            label = tk.Label(win, textvariable=pct_var, fg="white", bg="#2b2b2b",
                             font=("Segoe UI", 9))
            label.pack(pady=(6, 0))

            slider = tk.Scale(
                win, from_=100, to=0, orient=tk.VERTICAL,
                length=slider_len, width=14, sliderlength=18,
                resolution=1, showvalue=False,
                label="Receiver Volume",
                bg="#2b2b2b", fg="white", troughcolor="#333",
                highlightthickness=2, highlightcolor="#6688cc", bd=0,
                activebackground="#ccc",
            )
            slider.set(int(current))
            slider.pack(padx=4)

            _debounce_id = [None]

            def _on_change(val):
                v = int(float(val))
                pct_var.set(f"{v}%")
                # Debounce: apply after 150ms of no change
                if _debounce_id[0]:
                    win.after_cancel(_debounce_id[0])
                _debounce_id[0] = win.after(150, lambda: _apply(v))

            def _apply(v):
                self._cfg.receiver_volume = float(v)
                self._cfg_store.save(self._cfg)
                if self._atv:
                    try:
                        self._schedule(self._atv.audio.set_volume(float(v)))
                    except Exception:
                        pass
                self._update_menu()

            slider.configure(command=_on_change)

            def _dismiss(event=None):
                try:
                    win.grab_release()
                    win.destroy()
                except Exception:
                    pass

            win.bind("<Escape>", _dismiss)
            # Close when clicking outside
            win.after(100, lambda: win.grab_set())
            win.bind("<FocusOut>", lambda e: win.after(50, lambda: _dismiss() if win.focus_get() is None else None))
            slider.focus_set()

        self._run_on_ui(_show)

    # ───────────────────────── Settings toggles ────────────────────────────

    def _toggle_autostart(self, icon=None, item=None):
        new_val = not self._cfg.autostart_with_windows
        try:
            set_autostart_enabled(new_val)
            self._cfg.autostart_with_windows = new_val
            self._cfg_store.save(self._cfg)
            log.info(f"Autostart with Windows: {new_val}")
        except Exception as e:
            log.warning(f"Failed to change autostart: {e}")
            self._set_status(f"Autostart failed: {e}", ICON_ERROR)
            self._show_error_dialog("Autostart Failed", str(e))
        self._update_menu()

    def _toggle_auto_connect(self, icon=None, item=None):
        self._cfg.auto_connect_on_launch = not self._cfg.auto_connect_on_launch
        self._cfg_store.save(self._cfg)
        log.info(f"Auto-connect on launch: {self._cfg.auto_connect_on_launch}")
        self._update_menu()

    def _toggle_auto_capture_preferred_app(self, icon=None, item=None):
        self._cfg.auto_capture_preferred_app = not self._cfg.auto_capture_preferred_app
        self._cfg_store.save(self._cfg)
        log.info(f"Auto-capture preferred app: {self._cfg.auto_capture_preferred_app}")
        self._update_menu()

    def _edit_preferred_apps_dialog(self, icon=None, item=None):
        def _show():
            win = self._create_dialog("Preferred Apps", "500x300")

            lf = tk.LabelFrame(win, text="Preferred app executable names (one per line)")
            lf.pack(padx=12, pady=(10, 0), fill=tk.BOTH, expand=True)
            txt = tk.Text(lf, height=8, width=52)
            txt.pack(padx=4, pady=4, fill=tk.BOTH, expand=True)
            txt.insert("1.0", "\n".join(self._cfg.preferred_app_names or ["spotify.exe"]))
            win._focus_target = txt

            def _tab_forward(e):
                e.widget.tk_focusNext().focus()
                return "break"

            def _tab_backward(e):
                e.widget.tk_focusPrev().focus()
                return "break"

            txt.bind("<Tab>", _tab_forward)
            txt.bind("<Shift-Tab>", _tab_backward)

            def on_save():
                raw = txt.get("1.0", "end").strip().splitlines()
                names = [x.strip() for x in raw if x.strip()]
                if not names:
                    messagebox.showerror("Invalid", "Please enter at least one name.", parent=win)
                    txt.focus_set()
                    return
                self._cfg.preferred_app_names = names
                self._cfg_store.save(self._cfg)
                log.info(f"Preferred apps set: {names}")
                self._update_menu()
                self._close_dialog(win)

            txt.bind("<Control-Return>", lambda e: on_save())
            tk.Label(win, text="Ctrl+Enter to save", font=("", 8), takefocus=0).pack(pady=(4, 0))
            btn_frame = tk.Frame(win, takefocus=0)
            btn_frame.pack(pady=(4, 0))
            tk.Button(btn_frame, text="Save", command=on_save, width=16).pack(side=tk.LEFT, padx=4)
            tk.Button(
                btn_frame,
                text="Cancel",
                command=lambda: self._close_dialog(win),
                width=16,
            ).pack(side=tk.LEFT, padx=4)

        self._run_on_ui(_show)

    # ───────────────────────── Misc helpers ────────────────────────────────

    def _open_logs(self, icon=None, item=None):
        try:
            os.startfile(str(LOG_PATH))
        except Exception as e:
            log.warning(f"Failed to open logs: {e}")

    def _show_status_dialog(self, icon=None, item=None):
        target = self._cfg.selected_device_name or "(none)"
        source = self._target_name or "All Audio"
        receiver_volume = self._cfg.receiver_volume if self._cfg.receiver_volume is not None else 50.0
        msg = (
            f"Status: {self._status}\n"
            f"Target Device: {target}\n"
            f"Audio Source: {source}\n"
            f"Receiver Volume: {receiver_volume:.0f}%\n"
            f"Local Gain: {self._gain_db:+} dB"
        )
        if self._last_error:
            msg += f"\nLast Error: {self._last_error}"
        self._show_info_dialog("Current Status", msg)

    def _about(self, icon=None, item=None):
        msg = (
            f"{APP_NAME} v{APP_VERSION}\n\n"
            "Mirrors Windows audio to an AirPlay receiver via RAOP.\n\n"
            f"Config: {CONFIG_PATH}\n"
            f"Logs: {LOG_PATH}"
        )
        self._show_info_dialog(APP_NAME, msg)

    # ───────────────────────── Tray callbacks ──────────────────────────────

    def _register_hotkey(self):
        try:
            import keyboard
        except ImportError:
            log.warning("keyboard module not available – hotkey disabled")
            return
        self._unregister_hotkey()
        if self._cfg.hotkey_toggle:
            try:
                keyboard.add_hotkey(self._cfg.hotkey_toggle, self._toggle_streaming)
                log.info(f"Global hotkey registered: {self._cfg.hotkey_toggle}")
            except Exception:
                log.exception("Failed to register hotkey")

    def _unregister_hotkey(self):
        try:
            import keyboard
            keyboard.unhook_all_hotkeys()
        except Exception:
            pass

    def _change_hotkey_dialog(self, icon=None, item=None):
        def _show():
            win = self._create_dialog("Change Hotkey", "350x120")
            tk.Label(win, text="Press new hotkey combo, then click Save.\nClear the field to disable.").pack(pady=(8, 4))
            entry = tk.Entry(win, justify="center", font=("Segoe UI", 11))
            entry.insert(0, self._cfg.hotkey_toggle or "")
            entry.pack(padx=16, fill="x")
            entry.focus_set()
            win._focus_target = entry

            def _save():
                new_hk = entry.get().strip()
                self._cfg.hotkey_toggle = new_hk
                self._config_store.save(self._cfg)
                self._register_hotkey()
                self._update_menu()
                self._close_dialog(win)

            btn_frame = tk.Frame(win)
            btn_frame.pack(pady=8)
            tk.Button(btn_frame, text="Save", width=10, command=_save).pack(side="left", padx=4)
            tk.Button(btn_frame, text="Cancel", width=10, command=lambda: self._close_dialog(win)).pack(side="left", padx=4)

        if self._ui_root:
            self._ui_root.after(0, _show)

    def _toggle_streaming(self, icon=None, item=None):
        if self._streaming:
            self._stop_streaming()
        else:
            self._start_streaming()

    def _start_streaming(self, icon=None, item=None):
        if self._streaming:
            return
        self._stop_requested.clear()
        self._capture_stop.clear()
        self._restart_capture.clear()
        self._stream_future = self._schedule(self._stream_loop())

    def _stop_streaming(self, icon=None, item=None):
        with self._state_lock:
            if not self._streaming and self._status == "Idle":
                return
            self._streaming = False
            atv = self._atv
            self._atv = None
        self._stop_requested.set()
        self._capture_stop.set()
        self._restart_capture.set()
        if self._source:
            try:
                self._schedule(self._source.close())
            except Exception:
                pass
        if self._stream_future:
            try:
                self._stream_future.cancel()
            except Exception:
                pass
            self._stream_future = None
        self._set_status("Idle", ICON_IDLE)
        self._update_menu()

    def _quit(self, icon=None, item=None):
        self._shutdown.set()
        self._unregister_hotkey()
        self._stop_streaming()

        if self._enumerator and self._device_client:
            try:
                self._enumerator.UnregisterEndpointNotificationCallback(self._device_client)
            except Exception:
                pass
        if self._async_loop:
            self._async_loop.call_soon_threadsafe(self._async_loop.stop)
        if self._async_thread:
            self._async_thread.join(timeout=3)
        if self._ui_root:
            try:
                self._ui_root.after(0, self._ui_root.quit)
            except Exception:
                pass
        if self._ui_thread:
            self._ui_thread.join(timeout=3)
        if self._icon:
            self._icon.stop()

    def _set_status(self, status: str, icon_img):
        self._status = status
        if icon_img is ICON_ERROR and status != "Reconnecting...":
            self._last_error = status
        elif icon_img is not ICON_ERROR:
            self._last_error = None
        if self._icon:
            try:
                self._icon.icon = icon_img
                self._icon.title = f"AirPlay: {status}"
            except Exception:
                pass
        try:
            if icon_img is ICON_ERROR:
                winsound.MessageBeep(winsound.MB_ICONHAND)
            elif icon_img is ICON_STREAMING:
                winsound.MessageBeep(winsound.MB_OK)
        except Exception:
            pass

    def _update_menu(self):
        if self._icon:
            try:
                self._icon.menu = self._build_menu()
            except Exception:
                pass

    # ───────────────────────── Menu building ───────────────────────────────

    def _build_menu(self):
        device_items = [
            pystray.MenuItem("Rescan Devices", lambda icon=None, item=None: self._scan_devices()),
            pystray.MenuItem("Add by IP...", self._manual_add_device_dialog),
            pystray.Menu.SEPARATOR,
        ]
        devices = self._get_devices_sorted()
        if not devices:
            device_items.append(pystray.MenuItem("(no devices found)", None, enabled=False))
        else:
            for dev in devices[:25]:
                label = f"{dev.name} ({dev.address})"
                device_items.append(
                    pystray.MenuItem(
                        label,
                        self._select_device(dev.identifier),
                        checked=lambda item, i=dev.identifier: self._cfg.selected_device_id == i,
                        radio=True,
                    )
                )
            if len(devices) > 25:
                device_items.append(pystray.MenuItem(f"More Devices ({len(devices)-25})...", self._open_device_picker_dialog))
        device_submenu = pystray.Menu(*device_items)

        source_items = [
            pystray.MenuItem("All Audio", self._set_audio_source_all, checked=lambda item: self._target_pid is None, radio=True),
            pystray.Menu.SEPARATOR,
        ]
        sessions = self._get_audio_sessions()
        for pid, name in sessions[:25]:
            source_items.append(
                pystray.MenuItem(
                    f"{name} ({pid})",
                    self._set_audio_source_pid(pid, name),
                    checked=lambda item, p=pid: self._target_pid == p,
                    radio=True,
                )
            )
        if len(sessions) > 25:
            source_items.append(pystray.MenuItem(f"More Sources ({len(sessions)-25})...", self._open_audio_source_picker_dialog))
        if len(source_items) == 2:
            source_items.append(pystray.MenuItem("(no active sessions)", None, enabled=False))
        source_submenu = pystray.Menu(*source_items)

        gain_items = [
            pystray.MenuItem(
                f"{db:+} dB",
                self._set_gain(db),
                checked=lambda item, db=db: self._gain_db == db,
                radio=True,
            )
            for db in GAIN_PRESETS
        ]
        gain_submenu = pystray.Menu(
            *gain_items,
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Adjust...", self._open_gain_slider),
            pystray.MenuItem(
                "Limiter (prevent clipping)",
                self._toggle_limiter,
                checked=lambda item: self._enable_limiter,
            ),
        )

        rv = self._cfg.receiver_volume if self._cfg.receiver_volume is not None else 50.0

        bind_submenu = pystray.Menu(
            pystray.MenuItem("Auto (recommended)", self._set_bind_mode("auto"), checked=lambda item: self._cfg.bind_mode == "auto", radio=True),
            pystray.MenuItem("Manual...", self._set_bind_mode("manual"), checked=lambda item: self._cfg.bind_mode == "manual", radio=True),
            pystray.MenuItem("None (OS default)", self._set_bind_mode("none"), checked=lambda item: self._cfg.bind_mode == "none", radio=True),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem(f"Current: {self._binder.local_ip or 'n/a'}", None, enabled=False),
        )

        settings_submenu = pystray.Menu(
            pystray.MenuItem("Start with Windows", self._toggle_autostart, checked=lambda item: self._cfg.autostart_with_windows),
            pystray.MenuItem("Auto-connect on launch", self._toggle_auto_connect, checked=lambda item: self._cfg.auto_connect_on_launch),
            pystray.MenuItem("Auto-capture preferred app", self._toggle_auto_capture_preferred_app, checked=lambda item: self._cfg.auto_capture_preferred_app),
            pystray.MenuItem("Edit preferred apps...", self._edit_preferred_apps_dialog),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Network binding", bind_submenu),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem(f"Toggle Hotkey: {self._cfg.hotkey_toggle or '(disabled)'}", self._change_hotkey_dialog),
        )

        toggle_label = "Stop Streaming" if self._streaming else "Start Streaming"
        toggle_action = self._stop_streaming if self._streaming else self._start_streaming
        target_name = self._cfg.selected_device_name or "(none)"
        source_label = self._target_name or "All Audio"

        return pystray.Menu(
            pystray.MenuItem(f"Status: {self._status}", None, enabled=False),
            pystray.MenuItem("View Status...", self._show_status_dialog),
            pystray.MenuItem(toggle_label, toggle_action),
            pystray.MenuItem(f"Target Device: {target_name}", device_submenu),
            pystray.MenuItem(f"Audio Source: {source_label}", source_submenu),
            pystray.MenuItem(f"Receiver Volume: {rv:.0f}%", self._open_receiver_volume_popup),
            pystray.MenuItem(f"Local Gain: {self._gain_db:+} dB", gain_submenu),
            pystray.MenuItem("Settings", settings_submenu),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Open Logs", self._open_logs),
            pystray.MenuItem("About", self._about),
            pystray.MenuItem("Quit", self._quit),
        )

    # ───────────────────────── Entry point ────────────────────────────────

    def run(self) -> None:
        if "--debug" in sys.argv:
            log.setLevel(logging.DEBUG)
            logging.getLogger("pyatv").setLevel(logging.INFO)
            logging.getLogger("PIL").setLevel(logging.INFO)
            logging.getLogger("comtypes").setLevel(logging.INFO)
        if "--debug-pyatv" in sys.argv:
            logging.getLogger("pyatv").setLevel(logging.DEBUG)

        self._ui_thread = threading.Thread(target=self._run_ui_loop, daemon=True, name="tk-ui")
        self._ui_thread.start()
        self._ui_ready.wait(timeout=2)

        # Start asyncio thread
        self._async_thread = threading.Thread(target=self._run_async_loop, daemon=True, name="asyncio")
        self._async_thread.start()

        self._async_ready.wait(timeout=5)

        # Register device change listener
        self._device_client = DeviceChangeClient(self._restart_capture)
        try:
            self._enumerator = AudioUtilities.GetDeviceEnumerator()
            self._enumerator.RegisterEndpointNotificationCallback(self._device_client)
            log.info("Default output device listener registered")
        except Exception as e:
            log.warning(f"Could not register device listener: {e}")

        # Initial scan (non-blocking)
        self._scan_devices()

        self._icon = pystray.Icon(
            "trayplay",
            icon=ICON_IDLE,
            title="AirPlay: Idle",
            menu=self._build_menu(),
        )
        log.info("AirPlay tray app started")

        self._register_hotkey()

        if self._cfg.auto_connect_on_launch:
            self._start_streaming()

        self._icon.run()


if __name__ == "__main__":
    AirPlayTray().run()
