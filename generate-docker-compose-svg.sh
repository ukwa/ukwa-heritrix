#!/bin/sh
docker run --rm -it -v $(pwd):/input pmsipilot/docker-compose-viz render -m graphviz --no-volumes -f -o docker-compose.svg docker-compose.yml
