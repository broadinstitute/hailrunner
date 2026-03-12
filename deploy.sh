#!/bin/bash
set -ex

cd "$(dirname "$0")"

git add -A
git commit -m "deploy $(date +%Y-%m-%d-%H%M%S)"

cd hailrunner/docker
bash push.sh
