#!/bin/sh
#
# 6
#
# Build BL modules
git clone https://github.com/ukwa/bl-heritrix-modules.git bl-heritrix-modules
cd /bl-heritrix-modules
mvn install -DskipTests
cd /

# And install module and it's dependencies:
cp /bl-heritrix-modules/target/bl-heritrix-modules-*jar-with-dependencies.jar ./h3-bin/lib
