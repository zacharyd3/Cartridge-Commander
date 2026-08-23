#!/bin/bash
# identify-devices.sh -- pinpoint which /dev/sgN and /dev/nstN nodes are the
# TL2000's changer and tape drive, so you can update the container's device
# mappings after Unraid renumbers them on reboot.
#
# Runs entirely against the host. Paste it into Unraid's **User Scripts**
# plugin as a new script and hit "Run Script" (Run in Background is fine),
# then read the output. Nothing here moves the picker arm or touches tape --
# it only queries SCSI inquiry data, so it's safe to run any time, even
# mid-backup.
#
# Background on *why* this is needed lives in the repo README under
# "Stable device paths"; the durable fix is the udev rule in
# udev-rules/99-tl2000.rules. This script both reports the current numbering
# AND tells you whether that udev rule is installed and resolving.

# Deliberately no `set -e`: a single missing tool or an unreadable device
# node should degrade to a warning, not abort the whole report.

# A device's role comes from its SCSI Peripheral Device Type (PDT):
# 8 = Medium Changer (the changer), 1 = Sequential-access (the tape drive).
# We match on the human-readable PDT text sg_inq/lsscsi print, with a
# vendor/model fallback for the TL2000's IBM 3573-TL and ULT3580 drive.

bold()  { printf '\033[1m%s\033[0m\n' "$*"; }
warn()  { printf '  \033[33m! %s\033[0m\n' "$*"; }
ok()    { printf '  \033[32m%s\033[0m\n' "$*"; }

have() { command -v "$1" >/dev/null 2>&1; }

# Discovered mappings, filled in as we scan.
CHANGER_SG=""
DRIVE_SG=""
DRIVE_ST=""     # rewinding node, e.g. /dev/st0
DRIVE_NST=""    # non-rewinding node, e.g. /dev/nst0

bold "== Cartridge Commander :: tape device identification =="
echo  "   host: $(uname -n)   date: $(date)"
echo

# -----------------------------------------------------------------------------
# 1. Kernel's own record of what attached where. Always available, no extra
#    packages, and it's the fastest sanity check for "did the library even
#    enumerate this boot".
# -----------------------------------------------------------------------------
bold "-- dmesg: SCSI generic / tape attachments --"
if dmesg 2>/dev/null | grep -iE "Attached scsi generic|st [0-9].*Attached|Attached .*tape" >/dev/null; then
    dmesg 2>/dev/null | grep -iE "Attached scsi generic|st [0-9].*Attached|Attached .*tape" \
        | sed 's/^/  /'
else
    warn "no 'Attached scsi generic' lines in dmesg (ring buffer may have rotated -- not fatal, the scan below is authoritative)"
fi
echo

# -----------------------------------------------------------------------------
# 2. lsscsi -g is the clean path: it prints type, vendor, model AND both the
#    tape node (/dev/st*) and the generic node (/dev/sg*) on one line, so we
#    never have to guess which sg pairs with which st. Present on stock Unraid.
# -----------------------------------------------------------------------------
if have lsscsi; then
    bold "-- lsscsi -g (type / vendor / model / node / sg) --"
    lsscsi -g 2>/dev/null | grep -Ei "mediumx|tape|changer" | sed 's/^/  /'
    [ -z "$(lsscsi -g 2>/dev/null | grep -Ei 'mediumx|tape|changer')" ] && \
        warn "lsscsi found no medium-changer or tape devices -- is the library powered on and cabled?"
    echo

    # Parse lsscsi -g for the actual node paths. Fields:
    #   [H:C:T:L] type vendor model rev /dev/stX /dev/sgY
    # For a changer there's no st node, so the sg node is the last field.
    while read -r line; do
        type=$(printf '%s' "$line" | awk '{print $2}')
        # /dev/sg* is always the final field; /dev/st* (if any) is second-to-last.
        sgnode=$(printf '%s' "$line" | grep -oE '/dev/sg[0-9]+' | tail -n1)
        stnode=$(printf '%s' "$line" | grep -oE '/dev/st[0-9]+' | tail -n1)
        case "$type" in
            mediumx*|*changer*)
                [ -n "$sgnode" ] && CHANGER_SG="$sgnode"
                ;;
            tape*)
                [ -n "$sgnode" ] && DRIVE_SG="$sgnode"
                [ -n "$stnode" ] && DRIVE_ST="$stnode"
                ;;
        esac
    done < <(lsscsi -g 2>/dev/null | grep -Ei "mediumx|tape|changer")
else
    warn "lsscsi not installed on the host -- falling back to per-node sg_inq below"
    echo
fi

# -----------------------------------------------------------------------------
# 3. Per-/dev/sg* inquiry. This is the authoritative confirmation (matches the
#    README's `sg_inq /dev/sgN`) and also our fallback when lsscsi is absent.
#    We read the PDT to classify, and vendor/model/serial to identify.
# -----------------------------------------------------------------------------
bold "-- per-device SCSI inquiry (/dev/sg*) --"
shopt -s nullglob
sg_nodes=(/dev/sg*)
shopt -u nullglob
if [ ${#sg_nodes[@]} -eq 0 ]; then
    warn "no /dev/sg* nodes exist at all -- the SCSI generic driver saw nothing this boot"
fi

for sg in "${sg_nodes[@]}"; do
    vendor=""; model=""; serial=""; pdt=""
    if have sg_inq; then
        inq=$(sg_inq "$sg" 2>/dev/null)
        vendor=$(printf '%s' "$inq" | sed -nE 's/.*[Vv]endor identification:[[:space:]]*//p' | head -n1)
        model=$( printf '%s' "$inq" | sed -nE 's/.*[Pp]roduct identification:[[:space:]]*//p' | head -n1)
        serial=$(printf '%s' "$inq" | sed -nE 's/.*[Uu]nit serial number:[[:space:]]*//p' | head -n1)
        # PDT prints like "Peripheral device type: sequential access" or a code.
        pdt=$(printf '%s' "$inq" | sed -nE 's/.*[Pp]eripheral device type:[[:space:]]*//p' | head -n1)
    elif have sg_readcap || have lsscsi; then
        : # nothing else per-node; classification came from lsscsi above
    fi

    # Classify by PDT text if sg_inq gave it; otherwise lean on the model string.
    role="?"
    case "$pdt" in
        *edium*hanger*|*8*) role="CHANGER"; [ -z "$CHANGER_SG" ] && CHANGER_SG="$sg" ;;
        *equential*|*1*)    role="DRIVE";   [ -z "$DRIVE_SG"   ] && DRIVE_SG="$sg"   ;;
    esac
    if [ "$role" = "?" ]; then
        case "$model" in
            *3573*|*TL*)          role="CHANGER"; [ -z "$CHANGER_SG" ] && CHANGER_SG="$sg" ;;
            *ULT3580*|*LTO*|*HH*) role="DRIVE";   [ -z "$DRIVE_SG"   ] && DRIVE_SG="$sg"   ;;
        esac
    fi

    printf '  %-10s  %-8s  vendor=%-9s model=%-14s serial=%s\n' \
        "$sg" "$role" "${vendor:-?}" "${model:-?}" "${serial:-?}"
done
have sg_inq || warn "sg_inq not installed on the host -- vendor/model/serial columns above are blank; run this inside the container (which ships sg3_utils) for full detail, or trust the lsscsi lines above"
echo

# -----------------------------------------------------------------------------
# 4. Derive the non-rewinding tape node the app actually wants (TL_TAPE).
#    /dev/nstN is the non-rewinding twin of /dev/stN. If lsscsi gave us stN,
#    nstN is the same number with an 'n' prefix.
# -----------------------------------------------------------------------------
if [ -n "$DRIVE_ST" ]; then
    num=${DRIVE_ST#/dev/st}
    cand="/dev/nst${num}"
    [ -e "$cand" ] && DRIVE_NST="$cand"
fi
if [ -z "$DRIVE_NST" ]; then
    # Fallback: only one tape drive in a TL2000, so if exactly one nst* exists,
    # that's it.
    shopt -s nullglob
    nsts=(/dev/nst*)
    shopt -u nullglob
    [ ${#nsts[@]} -eq 1 ] && DRIVE_NST="${nsts[0]}"
fi

# -----------------------------------------------------------------------------
# 5. Is the durable udev rule installed and resolving? If so, the whole
#    renumbering problem is already solved and the symlinks should be used
#    instead of raw sgN paths.
# -----------------------------------------------------------------------------
bold "-- udev symlinks (the permanent fix) --"
udev_ok=1
for link in /dev/tape-changer /dev/tape-drive-sg; do
    if [ -L "$link" ]; then
        ok "$link -> $(readlink -f "$link")"
    else
        warn "$link missing -- udev rule not installed/loaded (see README 'Stable device paths')"
        udev_ok=0
    fi
done
echo

# -----------------------------------------------------------------------------
# 6. The payoff: exact values to paste into the container's Edit page.
# -----------------------------------------------------------------------------
bold "== Recommended container settings =="
if [ "$udev_ok" = 1 ]; then
    ok "udev rule is active -- prefer the stable symlinks; these don't change across reboots:"
    echo "    TL_CHANGER = /dev/tape-changer"
    echo "    SG_DEVICE  = /dev/tape-drive-sg    (only if using sg_logs health polling)"
    printf  "    TL_TAPE    = %s\n" "${DRIVE_NST:-/dev/nstN -- not detected; check dmesg above}"
    echo
    echo "  Device mappings (Container Device -> Host Device):"
    echo "    /dev/tape-changer   -> /dev/tape-changer"
    echo "    /dev/nst0           -> ${DRIVE_NST:-/dev/nstN}"
    echo "    /dev/tape-drive-sg  -> /dev/tape-drive-sg   (optional, health polling)"
else
    warn "udev rule not active -- use the raw nodes below FOR NOW, but install the udev"
    warn "rule (udev-rules/99-tl2000.rules) so you stop having to re-run this every reboot."
    echo
    echo "  Env vars:"
    printf  "    TL_CHANGER = %s\n" "${CHANGER_SG:-/dev/sgN -- not detected; see scan above}"
    printf  "    TL_TAPE    = %s\n" "${DRIVE_NST:-/dev/nstN -- not detected; see scan above}"
    printf  "    SG_DEVICE  = %s    (only if using sg_logs health polling)\n" "${DRIVE_SG:-/dev/sgN}"
    echo
    echo "  Device mappings (Container Device -> Host Device):"
    printf  "    /dev/tape-changer   -> %s\n" "${CHANGER_SG:-/dev/sgN}"
    printf  "    /dev/nst0           -> %s\n" "${DRIVE_NST:-/dev/nstN}"
    printf  "    /dev/tape-drive-sg  -> %s   (optional)\n" "${DRIVE_SG:-/dev/sgN}"
fi
echo
bold "Done. Update the container (Docker > CartridgeCommander > Edit), set the"
echo  "values above, and hit Apply."
