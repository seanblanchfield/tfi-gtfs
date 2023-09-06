ARG BUILD_FROM=redis:alpine
FROM $BUILD_FROM

# Install requirements for add-on
RUN \
  apk add --no-cache \
    python3 \
    py3-pip \
    wget \
    unzip


# Add the source code
WORKDIR /app
ADD . /app/
RUN rm -rf /app/data/*
RUN python3 -m pip install -r requirements.txt
ENTRYPOINT [ "/app/entrypoint.sh" ]
