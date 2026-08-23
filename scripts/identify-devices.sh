#!/bin/bash
# identify-devices.sh -- pinpoint which /dev/sgN and /dev/nstN nodes are the
# TL2000's changer and tape drive, so you can update the container's settings
# after Unraid renumbers them on reboot.
#
# Paste into Unraid's **User Scripts** plugin as a new script and hit
# "Run Script". Read-only: it reads SCSI identity out of sysfs and never moves
# the picker arm or touches tape, so it is safe to run any time, even
# mid-backup.
#
# Detection reads /sys/class/scsi_generic/*/device/ -- vendor, model and the
# SCSI Peripheral Device Type (PDT) as a plain integer, where 8 = Medium
# Changer and 1 = Sequential-access. sysfs is always present on Linux, so this
# needs no lsscsi/sg_inq and has no output-format parsing to go stale.
#
# The durable fix for renumbering is the udev rule in
# udev-rules/99-tl2000.rules (see README "Stable device paths"); this script
# reports whether that rule is active and adjusts its advice accordingly.

# Deliberately no `set -e`: a missing tool or unreadable node should degrade to
# a warning, not abort the report.

PDT_TAPE=1
PDT_CHANGER=8

# Colors only when attached to a terminal. Unraid's User Scripts panel captures
# stdout through a pipe and renders ANSI escapes as literal "[1m" garbage, so
# everything below must stay readable as plain text.
if [ -t 1 ] && [ -z "$NO_COLOR" ]; then
    C_B=$'\033[1m'; C_DIM=$'\033[2m'; C_OK=$'\033[32m'; C_WARN=$'\033[33m'; C_R=$'\033[0m'
else
    C_B=""; C_DIM=""; C_OK=""; C_WARN=""; C_R=""
fi

rule()    { printf '%s\n' "----------------------------------------------------------------------"; }
heading() { printf '\n%s%s%s\n' "$C_B" "$1" "$C_R"; rule; }
warn()    { printf '  %s! %s%s\n' "$C_WARN" "$1" "$C_R"; }
good()    { printf '  %s%s%s\n'   "$C_OK"   "$1" "$C_R"; }
note()    { printf '  %s%s%s\n'   "$C_DIM"  "$1" "$C_R"; }

# Human label for a SCSI PDT, used in the reference scan.
pdt_name() {
    case "$1" in
        0)  echo "disk" ;;      1)  echo "tape" ;;      2)  echo "printer" ;;
        3)  echo "processor" ;; 4)  echo "worm" ;;      5)  echo "cd/dvd" ;;
        6)  echo "scanner" ;;   7)  echo "optical" ;;   8)  echo "changer" ;;
        9)  echo "comms" ;;     12) echo "raid" ;;      13) echo "enclosure" ;;
        14) echo "rbc" ;;       17) echo "osd" ;;       *)  echo "type $1" ;;
    esac
}

CHANGER_SG=""; CHANGER_ID=""; CHANGER_ADDR=""
DRIVE_SG="";   DRIVE_ID="";   DRIVE_ADDR="";  DRIVE_ST="";  DRIVE_NST=""
SCAN_ROWS=""

printf '%s\n' "======================================================================"
printf '%s  Cartridge Commander  --  Tape Device Identification%s\n' "$C_B" "$C_R"
printf '%s\n' "======================================================================"
printf '  host: %-24s %s\n' "$(uname -n)" "$(date '+%Y-%m-%d %H:%M:%S %Z')"

# ---------------------------------------------------------------------------
# Scan every scsi_generic node via sysfs.
# ---------------------------------------------------------------------------
sg_names=$(ls /sys/class/scsi_generic/ 2>/dev/null | sed 's/^sg//' | sort -n)

for n in $sg_names; do
    sg="sg${n}"
    d="/sys/class/scsi_generic/${sg}/device"
    [ -d "$d" ] || continue

    vendor=$(tr -d '\0' < "$d/vendor" 2>/dev/null | sed 's/[[:space:]]*$//;s/^[[:space:]]*//')
    model=$( tr -d '\0' < "$d/model"  2>/dev/null | sed 's/[[:space:]]*$//;s/^[[:space:]]*//')
    pdt=$(   tr -d '\0' < "$d/type"   2>/dev/null | sed 's/[[:space:]]//g')
    addr=$(basename "$(readlink -f "$d" 2>/dev/null)" 2>/dev/null)

    # The kernel's tape node for this device, if it has one (drives only).
    st=""
    if [ -d "$d/scsi_tape" ]; then
        st=$(ls "$d/scsi_tape" 2>/dev/null | grep -xE 'st[0-9]+' | head -n1)
    fi

    case "$pdt" in
        "$PDT_CHANGER")
            CHANGER_SG="/dev/$sg"; CHANGER_ID="$vendor $model"; CHANGER_ADDR="$addr" ;;
        "$PDT_TAPE")
            DRIVE_SG="/dev/$sg";   DRIVE_ID="$vendor $model";   DRIVE_ADDR="$addr"
            [ -n "$st" ] && DRIVE_ST="/dev/$st" ;;
    esac

    SCAN_ROWS="${SCAN_ROWS}$(printf '  %-10s %-11s %-9s %s' \
        "/dev/$sg" "$(pdt_name "${pdt:-?}")" "${vendor:-?}" "${model:-?}")
"
done

# The app wants the NON-rewinding node: /dev/nstN is the twin of /dev/stN.
if [ -n "$DRIVE_ST" ]; then
    cand="/dev/nst${DRIVE_ST#/dev/st}"
    [ -e "$cand" ] && DRIVE_NST="$cand"
fi
if [ -z "$DRIVE_NST" ]; then
    # Fallback: a TL2000 has a single drive, so a lone nst* node must be it.
    nsts=$(ls -d /dev/nst[0-9]* 2>/dev/null | grep -xE '/dev/nst[0-9]+')
    [ "$(printf '%s\n' "$nsts" | grep -c .)" = "1" ] && DRIVE_NST="$nsts"
fi

# ---------------------------------------------------------------------------
# The answer, up front.
# ---------------------------------------------------------------------------
heading "DETECTED TAPE HARDWARE"
if [ -z "$CHANGER_SG" ] && [ -z "$DRIVE_SG" ]; then
    warn "No medium changer (PDT 8) or tape drive (PDT 1) found."
    warn "Check the library is powered on, cabled, and finished initializing,"
    warn "then re-run. 'dmesg | grep -i scsi' will show what the kernel saw."
else
    printf '  %-9s %-24s %-11s %s\n' "ROLE" "VENDOR / MODEL" "DEVICE" "SCSI ADDR"
    [ -n "$CHANGER_SG" ] \
        && printf '  %-9s %-24s %-11s %s\n' "CHANGER" "$CHANGER_ID" "$CHANGER_SG" "$CHANGER_ADDR" \
        || warn "changer not found (PDT 8)"
    [ -n "$DRIVE_SG" ] \
        && printf '  %-9s %-24s %-11s %s\n' "DRIVE" "$DRIVE_ID" "$DRIVE_SG" "$DRIVE_ADDR" \
        || warn "tape drive not found (PDT 1)"
    [ -n "$DRIVE_NST" ] \
        && printf '  %-9s %-24s %-11s %s\n' "TAPE" "non-rewinding node" "$DRIVE_NST" "$DRIVE_ADDR" \
        || warn "no /dev/nst* node -- the st driver may not be loaded (modprobe st)"
fi

# ---------------------------------------------------------------------------
# udev status decides which paths we recommend.
# ---------------------------------------------------------------------------
udev_ok=1
[ -L /dev/tape-changer  ] || udev_ok=0
[ -L /dev/tape-drive-sg ] || udev_ok=0

if [ "$udev_ok" = 1 ]; then
    V_CHANGER="/dev/tape-changer"
    V_DRIVESG="/dev/tape-drive-sg"
else
    V_CHANGER="${CHANGER_SG:-/dev/sgN}"
    V_DRIVESG="${DRIVE_SG:-/dev/sgN}"
fi
V_TAPE="${DRIVE_NST:-/dev/nstN}"

# ---------------------------------------------------------------------------
# Copy-paste block. Values are listed one per line with no "host:container"
# arrow syntax anywhere -- a stray ':' pasted into TL_CHANGER produces a path
# that does not exist, and the app then reports an empty library rather than
# an obvious error.
# ---------------------------------------------------------------------------
heading "CONTAINER SETTINGS"
printf '  Set these in %sDocker > CartridgeCommander > Edit%s\n\n' "$C_B" "$C_R"

printf '  %sVariables%s -- type the bare path exactly as shown.\n' "$C_B" "$C_R"
printf '           No colons, no quotes, no arrows. ":%s" is NOT the same\n' "${CHANGER_SG:-/dev/sgN}"
printf '           path as "%s" and will fail with an empty library.\n\n' "${CHANGER_SG:-/dev/sgN}"
printf '      TL_CHANGER    %s\n' "$V_CHANGER"
printf '      TL_TAPE       %s\n' "$V_TAPE"
printf '      SG_DEVICE     %s      (only for sg_logs health polling)\n\n' "$V_DRIVESG"

printf '  %sDevices%s -- add one Device entry per line. Unraid uses the same\n' "$C_B" "$C_R"
printf '           path on both sides, so each value appears once.\n\n'
printf '      %s\n' "$V_CHANGER"
printf '      %s\n' "$V_TAPE"
printf '      %s      (only if SG_DEVICE is set)\n\n' "$V_DRIVESG"

printf '  Then click %sApply%s -- a plain Restart keeps the old settings, because\n' "$C_B" "$C_R"
printf '  a container is bound to the config it was created with.\n'

# ---------------------------------------------------------------------------
heading "STABLE NAMING (udev)"
if [ "$udev_ok" = 1 ]; then
    good "Active -- the values above are symlinks that survive reboots:"
    printf '    /dev/tape-changer  -> %s\n' "$(readlink -f /dev/tape-changer)"
    printf '    /dev/tape-drive-sg -> %s\n' "$(readlink -f /dev/tape-drive-sg)"
    note "Numbering can still shift, but the symlinks follow the hardware,"
    note "so after a reboot just restart the container -- no edits needed."
else
    warn "Not installed. /dev/sgN numbering is assigned in enumeration order at"
    warn "boot, so it shifts whenever storage is added, removed or power-cycled."
    warn "Until the rule is in place you must re-run this script after a reboot."
    printf '\n'
    note "To fix permanently (see README 'Stable device paths'):"
    note "  mkdir -p /boot/config/udev-rules"
    note "  cp udev-rules/99-tl2000.rules /boot/config/udev-rules/"
    note "  # then add the copy+reload lines to /boot/config/go"
fi

# ---------------------------------------------------------------------------
heading "FULL SCSI SCAN (reference)"
if [ -n "$SCAN_ROWS" ]; then
    printf '  %-10s %-11s %-9s %s\n' "DEVICE" "TYPE" "VENDOR" "MODEL"
    printf '%s' "$SCAN_ROWS"
else
    warn "no /dev/sg* nodes found -- the sg driver saw nothing this boot"
fi

printf '\n'
rule
printf '%sDone.%s\n' "$C_B" "$C_R"
