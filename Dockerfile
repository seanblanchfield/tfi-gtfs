ARG BUILD_FROM=redis:alpine
FROM $BUILD_FROM

ARG VERSION=1
# ARG SOURCE_ZIP_URL=https://github.com/seanblanchfield/tfi-gtfs/archive/refs/heads/main.zip

# Install requirements for add-on
RUN \
  apk add --no-cache \
    python3 \
    py3-pip \
    redis \
    wget \
    unzip


# Get the python app and install dependencies
# RUN wget $SOURCE_ZIP_URL
# RUN unzip main.zip
# RUN rm main.zip
# RUN mv tfi-gtfs-main/* .

# Add the source code
WORKDIR /app
ADD . /app/
RUN rm -rf /app/data/*
RUN python3 -m pip install -r requirements.txt
ENTRYPOINT [ "/app/entrypoint.sh" ]
