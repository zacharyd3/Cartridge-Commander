FROM python:3.11-slim

# mt-st    -> `mt`      (rewind/erase/status on the tape drive)
# mtx      -> `mtx`     (changer load/unload/inventory)
# sg3-utils-> `sg_logs` (drive health counters, optional via SG_DEVICE)
# mbuffer  -> smooths tar->tape writes; falls back to pv, then plain dd if absent
# curl     -> container HEALTHCHECK
RUN apt-get update && apt-get install -y --no-install-recommends \
    mt-st \
    mtx \
    sg3-utils \
    mbuffer \
    pv \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

RUN mkdir -p /var/lib/tl2000 /mnt/user /mnt/restore

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost:8080/healthz || exit 1

CMD ["python", "app.py"]
