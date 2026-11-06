# Spark 3.5 + the Python libs applyInPandasWithState needs (pandas, pyarrow).
# The Kafka + S3A connector jars are pulled at submit time via --packages (see spark_defaults.conf).
FROM apache/spark:3.5.3-python3

USER root
RUN pip install --no-cache-dir "pandas>=2.0,<2.3" "pyarrow>=14,<18" "numpy<2" pytest \
    && mkdir -p /tmp/ivy && chmod -R 777 /tmp/ivy
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
USER spark
WORKDIR /app
