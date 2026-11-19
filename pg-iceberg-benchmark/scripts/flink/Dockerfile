FROM flink:1.19
RUN cd /opt/flink/lib && M=https://repo1.maven.org/maven2 && \
    wget -q $M/org/apache/flink/flink-sql-connector-postgres-cdc/3.2.1/flink-sql-connector-postgres-cdc-3.2.1.jar && \
    wget -q $M/org/apache/iceberg/iceberg-flink-runtime-1.19/1.7.1/iceberg-flink-runtime-1.19-1.7.1.jar && \
    wget -q $M/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar && \
    wget -q $M/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
