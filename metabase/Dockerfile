# Stage 1 Build generates the python package
FROM python:3.10-buster as python_builder

RUN git clone https://github.com/dveloz/movi-duckdb.git /home/movi-duckdb && \
    cd /home/movi-duckdb && \
    pip install -r requirements.txt


# Stage 2 Builds the metabase image
FROM openjdk:19-buster

ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY


ENV MB_PLUGINS_DIR /home/plugins/
ENV AWS_ACCESS_KEY_ID $AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY $AWS_SECRET_ACCESS_KEY
ENV MODE prod

ADD https://downloads.metabase.com/v0.46.2/metabase.jar /home
ADD https://github.com/AlexR2D2/metabase_duckdb_driver/releases/download/0.2.0/duckdb.metabase-driver.jar /home/plugins/

# We mount the python image unto the metabase image
COPY --from=python_builder / /

RUN chmod 744 /home/plugins/duckdb.metabase-driver.jar && chmod 744 /home/movi-duckdb/mb_init.sh

# ToDo: Create a script that generates a duckdb file using the movi-etl private repo
CMD ["sh", "-c", "/home/movi-duckdb/mb_init.sh"]
