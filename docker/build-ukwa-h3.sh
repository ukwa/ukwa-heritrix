#!/bin/sh

mkdir h3-build
cd h3-build
git clone https://github.com/ukwa/heritrix3.git
cd heritrix3
git checkout frontier-threadsafe-workqueues
echo "VERSION CHECK"
git log -1 --name-status
mvn install -DskipTests
cd ../..
unzip h3-build/heritrix3/dist/target/heritrix-3.3.0-SNAPSHOT-dist.zip
rm -fr h3-build

