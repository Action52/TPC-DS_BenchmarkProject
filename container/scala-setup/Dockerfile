FROM openjdk:8-alpine
ARG SCALA_VERSION
ARG SBT_VERSION
ENV SCALA_VERSION ${SCALA_VERSION:-2.11.12}
ENV SBT_VERSION ${SBT_VERSION:-1.2.7}

RUN \
    echo "$SCALA_VERSION $SBT_VERSION" && \
    mkdir -p /usr/lib/jvm/java-1.8-openjdk/jre && \
    touch /usr/lib/jvm/java-1.8-openjdk/jre/release && \
    apk add --no-cache bash && \
    apk add --no-cache curl && \
    curl -fsL http://downloads.typesafe.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.tgz | tar xfz - -C /usr/local && \
    ln -s /usr/local/scala-$SCALA_VERSION/bin/* /usr/local/bin/ && \
    scala -version && \
    scalac -version

RUN curl -fsL https://github.com/sbt/sbt/releases/download/v$SBT_VERSION/sbt-$SBT_VERSION.tgz | tar xfz - -C /usr/local && \
    $(mv /usr/local/spt-launcher-packaging-$SBT_VERSION /usr/local/sbt || true) \
    ln -s /usr/local/sbt/bin/* /usr/local/bin/ && \
    sbt sbt-version || sbt sbtVersion || true

WORKDIR /project

CMD "/usr/local/bin/sbt"
