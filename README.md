# Cartridge-Commander
Web-based management and automation for IBM TL2000 LTO tape libraries. Backup, restore, verify, schedule, and monitor via MQTT/Home Assistant integration.

## Running on Unraid (Docker > Add Container)

The container needs to shell out to `mtx`/`mt`/`sg_logs` on the host's SCSI
generic and tape devices, so it must be built as a real image rather than
run against a bare `python:3.11-slim` with dependencies installed at every
container start (slower restarts, and a hard failure if Docker Hub/apt
mirrors are unreachable when the container comes up).

### 1. Build the image on your Unraid box

SSH into Unraid, clone this repo somewhere under `/mnt/user` (e.g.
`/mnt/user/appdata/cartridge-commander-src`), then:

```bash
cd /mnt/user/appdata/cartridge-commander-src
docker build -t cartridge-commander:latest .
```

Re-run that `docker build` any time you pull new changes; Unraid's Docker
tab will pick up the local tag without needing a registry.

### 2. Add Container

In the Unraid GUI, **Docker > Add Container**, and fill in:

| Field | Value |
|---|---|
| Repository | `cartridge-commander:latest` |
| Network Type | Bridge |
| Port | Container `8080` → Host `8099` (or whatever's free) |

**Path mappings**

| Container Path | Host Path | Notes |
|---|---|---|
| `/var/lib/tl2000` | `/mnt/user/appdata/cartridge-commander` | index DB, schedules, backup records — must persist |
| `/mnt` (or narrower, e.g. `/mnt/user/backups`) | matching host path | wherever `BACKUP_ROOT`/`RESTORE_ROOT` point |

**Device mappings** — add one "Device" entry per device the app talks to:

| Container Device | Host Device |
|---|---|
| `/dev/tape-changer` | the changer — matches `TL_CHANGER` (see [Stable device paths](#stable-device-paths-recommended)) |
| `/dev/nst0` | the tape drive, non-rewinding — matches `TL_TAPE` |
| `/dev/tape-drive-sg` | drive's generic device, only if using `SG_DEVICE` health polling |

Passing explicit `--device` entries is enough for the container (running
as root by default) to read/write those nodes — **you do not need
`--privileged=true`**. Privileged mode hands the container every device
and capability on the host, which is unnecessary blast radius once the
specific devices are mapped, and can mask the day your `/dev/sg*` numbering
shifts (a privileged container silently still has access; a non-privileged
one fails loudly, telling you the mapping is stale).

### Stable device paths (recommended)

`/dev/sgN` numbering is **not stable** — it's assigned in enumeration order
at boot, so it can shift any time another SCSI/USB storage device is added,
removed, or the library gets power-cycled after other devices have already
come up. If your changer suddenly starts reporting a "Request Sense:
Illegal Request" error, or the app reports 0 tapes despite the library being
inventoried fine by hand, this renumbering is almost always why:

```bash
# find which /dev/sgN is currently the changer (PDT type 8) vs. the drive (type 1)
dmesg | grep -i "Attached scsi generic"
docker exec -it <container> sg_inq /dev/sgN   # confirm vendor/model per node
```

To stop chasing this every time, pin the devices by vendor/model instead of
by number using the udev rule in [`udev-rules/99-tl2000.rules`](udev-rules/99-tl2000.rules)
(edit the `model` match if your drive generation differs from the IBM
ULT3580-HH6 it ships with). On Unraid, `/etc/udev/rules.d` doesn't survive a
reboot — the OS is rebuilt from the flash drive each boot — so the rule has
to live under `/boot/config` and get installed by the `go` script:

```bash
mkdir -p /boot/config/udev-rules
cp udev-rules/99-tl2000.rules /boot/config/udev-rules/
```

Append to `/boot/config/go`:

```bash
mkdir -p /etc/udev/rules.d
cp /boot/config/udev-rules/*.rules /etc/udev/rules.d/
udevadm control --reload-rules
udevadm trigger
```

Run those three lines by hand once (or reboot) to pick up the rule
immediately. `/dev/tape-changer` and `/dev/tape-drive-sg` will now always
point at the right hardware, and Docker resolves `--device` symlinks at
container start — so after the next reboot, just restarting the container
picks up whatever `sgN` udev assigned this time, with no template edits.
Point `TL_CHANGER`/`SG_DEVICE` (and the matching `--device` flags) at these
symlinks instead of raw `/dev/sgN` paths.

**Variables** (env vars) — only set what differs from the defaults:

| Variable | Purpose |
|---|---|
| `TL_CHANGER` | changer `/dev/sgN` path (default `/dev/sg12`) |
| `TL_TAPE` | tape drive `/dev/nstN` path (default `/dev/nst0`) |
| `SG_DEVICE` | drive's `/dev/sgN` for `sg_logs` health data (optional) |
| `TL_WEBUI_PASSWORD` | sets a password on the web UI / API |
| `BACKUP_ROOT` | root directory backups are allowed to read from |
| `RESTORE_ROOT` | root directory restores are written to |
| `MQTT_HOST`, `MQTT_PORT`, `MQTT_USER`, `MQTT_PASS` | MQTT broker for Home Assistant discovery |
| `HA_URL`, `HA_TOKEN` | Home Assistant notifications (long-lived token) |
| `TL_POLL_SECONDS` | how often the UI polls changer/drive status |
| `ICON_PATH` | favicon/page icon served at `/icon.png` (default `/var/lib/tl2000/icon.png`) |
| `STARTUP_QUICK_SCAN` | run a quick barcode scan on container start so present tapes don't show as archived (default `true`) |

See the top of `app.py` for the full list of tunables (GFS retention,
verify sampling, pre/post-backup hooks, etc.) — every one is an
environment variable with a sane default.

The GFS retention keep counts (`GFS_DAILY_KEEP`, `GFS_WEEKLY_KEEP`,
`GFS_MONTHLY_KEEP`) seed the defaults, but they can also be edited and saved
from the **Settings → GFS Retention Policy** card in the UI; the saved values
are persisted and take precedence over the env vars.

### Health check

The image's `HEALTHCHECK` hits `GET /healthz`, which responds immediately
without touching the changer or drive — safe to poll even mid-backup.

### Updating

Restarting the container alone never picks up new code — a container is
bound to the exact image snapshot it was created from, not just the image
tag, so rebuilding `cartridge-commander:latest` doesn't change what an
already-running container is executing. [`scripts/update.sh`](scripts/update.sh)
stops the container, re-downloads the repo, and rebuilds the image tag;
it deliberately does **not** recreate/start the container itself. After it
finishes, go to **Docker > `CartridgeCommander` > Edit > Apply** (no fields
need changing) — this makes Unraid recreate the container from its own
saved template against the freshly built image and start it. Recreating
via the template this way (rather than scripting a hardcoded `docker run`)
means there's no risk of the update script's flags drifting out of sync
with whatever you've actually configured in the GUI.

This is a good candidate for Unraid's User Scripts plugin: paste the
script in as a new script and trigger it manually (a "Run Script" click)
rather than on a schedule — auto-pulling and swapping in unreviewed
upstream changes to a container driving physical hardware, unattended,
is worth avoiding.

## Local testing

`docker-compose.yml` mirrors the layout above for testing on a dev box
before rolling it into Unraid's Add Container UI:

```bash
docker compose up --build
```
