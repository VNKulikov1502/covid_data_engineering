FROM apache/superset:latest

USER root

RUN pip install uv
RUN uv pip install psycopg2-binary sqlalchemy-trino

USER superset