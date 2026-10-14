#!/bin/bash
# Restart the licensing container on the rehearsal VM, carrying its current
# env over from the RUNNING container (source of truth after first deploy).
#
# Usage (on the VM, sudo):
#   ./vm_restart.sh                          # same image, same env
#   ./vm_restart.sh gatelodge-licensing:2 -e NEW_VAR=value
#
# Env values are read via docker inspect from the running container —
# never hardcode secrets in this file. Mounts/ports/restart policy are
# fixed by the original deploy (see /home/coolify/licensing on the VM).
set -e

IMAGE="${1:-gatelodge-licensing:2}"
[ $# -gt 0 ] && shift || true

ENVS=$(docker inspect licensing --format '{{range .Config.Env}}{{println .}}{{end}}' \
  | grep -vE '^(PATH|LANG|GPG|PYTHON|HOSTNAME|HOME)=' \
  | sed 's/^/-e /')

echo "carrying env keys:"
docker inspect licensing --format '{{range .Config.Env}}{{println .}}{{end}}' \
  | cut -d= -f1 | grep -vE '^(PATH|LANG|GPG|PYTHON|HOSTNAME|HOME)' | tr '\n' ' '
echo

docker rm -f licensing
docker run -d --name licensing --restart unless-stopped -p 8100:8100 \
  $ENVS "$@" \
  -v licensing-data:/data \
  -v /home/coolify/licensing/binary:/app/binary:ro \
  -v /home/coolify/licensing/keys:/app/keys:ro \
  "$IMAGE"
sleep 4
echo "healthz: $(curl -s http://localhost:8100/healthz)"
