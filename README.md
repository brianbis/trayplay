# TrayPlay

TrayPlay is a Windows system tray app that streams Windows audio to AirPlay/RAOP receivers.

Current version: `0.2.0`

It can stream:

- All system audio using WASAPI loopback
- A single application's audio using Windows process loopback capture

It is designed for practical desktop use and includes low-latency tuning, device discovery, receiver volume control, multi-NIC binding controls, and tray-based configuration.

## Features

- AirPlay / RAOP device discovery on the local network
- Manual add-by-IP fallback for receivers that do not show up in multicast scans
- Stream all system audio or target a single application
- Receiver volume control through `pyatv` when supported
- Local gain control with limiter
- Auto-connect on launch
- Auto-capture of preferred apps
- Local IP binding controls for multi-network setups
- Accessible tray and dialog controls with keyboard-friendly dialogs
- Adaptive low-latency buffering

## Requirements

- Windows
- Python 3
- An AirPlay / RAOP-compatible receiver on your network

Python packages used by the script:

- `pyatv`
- `pyaudiowpatch`
- `pycaw`
- `pystray`
- `Pillow`
- `comtypes`

Install them with:

```powershell
pip install pyatv pyaudiowpatch pycaw pystray Pillow comtypes
```

## Running

From the repository folder:

```powershell
python .\trayplay.py
```

For app-level debug logging:

```powershell
python .\trayplay.py --debug
```

To also enable verbose `pyatv` logging:

```powershell
python .\trayplay.py --debug-pyatv
```

## Building a Windows Executable

TrayPlay includes a PyInstaller spec file for Windows builds.

Install build tools:

```powershell
pip install pyinstaller pyatv pyaudiowpatch pycaw pystray Pillow comtypes
```

Build:

```powershell
pyinstaller .\TrayPlay.spec
```

The packaged app will be created in `dist\TrayPlay\`.

## GitHub Actions

This repository includes a Windows GitHub Actions workflow that:

- builds TrayPlay on Windows
- packages the `dist\TrayPlay` folder as a zip
- uploads the zip as a workflow artifact
- attaches the zip to a GitHub Release when you push a version tag such as `v0.2.0`

## Usage

1. Launch the script.
2. Use the tray icon menu to select an AirPlay target.
3. Choose `Start Streaming`.
4. Optionally switch the audio source from `All Audio` to a specific application.
5. Use the tray menus for receiver volume, local gain, startup behavior, and preferred-app capture.

If your receiver does not appear automatically:

1. Open `Target Device`.
2. Choose `Rescan Devices`.
3. If needed, choose `Add by IP...` and enter the receiver's IP address.

## Notes

- The app currently ships as a single Python script.
- End-to-end latency still depends on the receiver and the underlying RAOP implementation, not just the local capture pipeline.
- Some Python builds do not include `audioop`; the script already falls back when it is unavailable.
- The process-loopback path uses Windows-only APIs and requires a supported Windows build.

## Repository Layout

- `trayplay.py` - main application script

## License

No license file is included yet. Add one before publishing if you want others to reuse or contribute to the code under explicit terms.
