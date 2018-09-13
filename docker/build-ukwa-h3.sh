#!/bin/sh

mkdir h3-build
cd h3-build
git clone https://github.com/ukwa/heritrix3.git
cd heritrix3
git checkout frontier-threadsafe-workqueues
mvn install -DskipTests
cd ../..
unzip h3-build/heritrix3/dist/target/heritrix-3.3.0-SNAPSHOT-dist.zip
rm -fr h3-build

