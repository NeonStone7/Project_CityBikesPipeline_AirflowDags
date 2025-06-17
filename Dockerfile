FROM quay.io/astronomer/astro-runtime:13.0.0

USER root
RUN pip install --no-cache-dir -r requirements.txt
USER airflow

