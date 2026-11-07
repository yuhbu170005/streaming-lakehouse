FROM apache/spark:4.1.3-python3
USER root
COPY streaming/jars.txt /tmp/jars.txt
RUN set -eux; while read -r artifact; do \
      curl -fsSLo "/opt/spark/jars/${artifact##*/}" "https://repo.maven.apache.org/maven2/$artifact"; \
    done < /tmp/jars.txt
WORKDIR /app
COPY streaming streaming
COPY schemas schemas
ENV PYTHONPATH=/app SPARK_LOCAL_IP=127.0.0.1
USER spark
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "768m", "/app/streaming/job.py"]
