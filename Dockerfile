FROM python:3.12-slim AS build

# Set path for Oracle client libraries
ENV LD_LIBRARY_PATH=/opt/lib/

RUN apt-get update && apt-get upgrade -y

# Install

# Install Oracle 19.x client libraries
RUN apt-get install -y unzip libaio1t64
RUN ln -s /usr/lib/x86_64-linux-gnu/libaio.so.1t64 /usr/lib/libaio.so.1
ENV INSTANTCLIENT_FILENAME=instantclient-basiclite-linux.x64-19.23.0.0.0dbru.zip
COPY vendor/$INSTANTCLIENT_FILENAME /
RUN unzip -j $INSTANTCLIENT_FILENAME -d /opt/lib/

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir --upgrade pip pipenv

RUN apt-get install -y git

COPY Pipfile* /
RUN pipenv install

ENTRYPOINT ["pipenv", "run", "hrqb"]
