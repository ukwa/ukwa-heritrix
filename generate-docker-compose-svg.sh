#!/bin/sh
docker run --rm -it -v $(pwd):/input pmsipilot/docker-compose-viz render -m graphviz -o docker-compose.svg docker-compose.yml
