FROM maven:3-jdk-8

MAINTAINER Andrew Jackson "anj@anjackson.net"

# Install extra software and resources:
RUN \
  curl -L -O http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz && \
  gunzip GeoLite2-City.mmdb.gz && \
  curl -L -O https://download.elastic.co/beats/filebeat/filebeat_1.0.0-rc1_amd64.deb && \
  dpkg -i filebeat_1.0.0-rc1_amd64.deb

# Add in the UKWA modules.
#
# We process the dependencies and source separately, so the JARs can be cached and 
# we don't need to download everything when we make code changes (unless we change the pom).
#
# First copy the pom in and update dependencies:
COPY pom.xml /bl-heritrix-modules/pom.xml
RUN mvn -B -f /bl-heritrix-modules/pom.xml -s /usr/share/maven/ref/settings-docker.xml dependency:resolve-plugins dependency:go-offline
#
# Then copy the code in and build it:
COPY src /bl-heritrix-modules/src
RUN cd /bl-heritrix-modules && \
    mvn -B -s /usr/share/maven/ref/settings-docker.xml -DskipTests install && \
    cp -r /bl-heritrix-modules/target/dist/heritrix-* /h3-bin

# Send in other required files:
COPY docker/filebeat.yml /etc/filebeat/filebeat.yml
COPY docker/logging.properties /h3-bin/conf/logging.properties
COPY docker/bin/* /h3-bin/bin/
COPY jobs /jobs

# Configure Heritrix options:
ENV FOREGROUND=true \
    JAVA_OPTS=-Xmx2g
    
#ENV JAVA_OPTS -Xmx2g -Dhttps.protocols=TLSv1,TLSv1.1,TLSv1.2

# Set up some defaults:
ENV MONITRIX_ENABLE=false \
    HERITRIX_USER=heritrix \
    HERITRIX_PASSWORD=heritrix \
    JOB_NAME=frequent

# Finish setup:
EXPOSE 8443

#VOLUME /jobs
VOLUME /output

#STOPSIGNAL TERM # Which is the default

# Hook in H3 runner script:
CMD [ "/h3-bin/bin/start" ]
