#!/bin/bash
# Pulls the latest code and rebuilds the Docker image, then stops the
# running container. It deliberately does NOT recreate/start the container
# itself -- do that from the Unraid GUI (Docker > <container> > Edit >
# Apply) so the container is always rebuilt from its own saved template
# rather than a hardcoded set of flags that can drift out of sync with it.
set -e

SRC=/mnt/user/appdata/cartridge-commander-src
NAME=CartridgeCommander

docker stop "$NAME"

cd "$SRC"
curl -L https://github.com/zacharyd3/Cartridge-Commander/archive/refs/heads/main.tar.gz | tar xz --strip-components=1
docker build -t cartridge-commander:latest .

echo "Image rebuilt. Go to Docker tab -> $NAME -> Edit -> Apply to recreate and start it against the new image."
