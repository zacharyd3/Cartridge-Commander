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
| `/dev/sg12` | `/dev/sg12` (the changer — matches `TL_CHANGER`) |
| `/dev/nst0` | `/dev/nst0` (the tape drive, non-rewinding — matches `TL_TAPE`) |
| `/dev/sg0` | `/dev/sg0` (drive's generic device, only if using `SG_DEVICE` health polling) |

Passing explicit `--device` entries is enough for the container (running
as root by default) to read/write those nodes — **you do not need
`--privileged=true`**. Privileged mode hands the container every device
and capability on the host, which is unnecessary blast radius once the
specific devices are mapped, and can mask the day your `/dev/sg*` numbering
shifts (a privileged container silently still has access; a non-privileged
one fails loudly, telling you the mapping is stale).

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

See the top of `app.py` for the full list of tunables (GFS retention,
verify sampling, pre/post-backup hooks, etc.) — every one is an
environment variable with a sane default.

### Health check

The image's `HEALTHCHECK` hits `GET /healthz`, which responds immediately
without touching the changer or drive — safe to poll even mid-backup.

## Local testing

`docker-compose.yml` mirrors the layout above for testing on a dev box
before rolling it into Unraid's Add Container UI:

```bash
docker compose up --build
```
