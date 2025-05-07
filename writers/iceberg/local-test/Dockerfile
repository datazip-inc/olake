FROM tabulario/spark-iceberg

ENV IVY2_CACHE_DIR=/root/.ivy2

RUN mkdir -p ${IVY2_CACHE_DIR} 

COPY ./data/ivy-cache ${IVY2_CACHE_DIR}

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf