FROM apache/superset:3.1.0

USER root
RUN pip install --no-cache-dir pymysql
USER superset

COPY superset_config.py /app/pythonpath/superset_config.py
COPY bootstrap.sh /app/bootstrap.sh

ENTRYPOINT ["/bin/bash", "/app/bootstrap.sh"]
