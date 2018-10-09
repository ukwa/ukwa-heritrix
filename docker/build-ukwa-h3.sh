#!/bin/sh

mkdir h3-build
cd h3-build
git clone https://github.com/ukwa/heritrix3.git
cd heritrix3

# git checkout frontier-threadsafe-workqueues
git checkout use-latest-checkpoint

echo "VERSION CHECK"
git log -1 --name-status
mvn -s /usr/share/maven/ref/settings-docker.xml install -DskipTests
cd contrib
mvn -s /usr/share/maven/ref/settings-docker.xml install -DskipTests
cd ../../..
unzip h3-build/heritrix3/dist/target/heritrix-3.3.0-SNAPSHOT-dist.zip
rm -fr h3-build

