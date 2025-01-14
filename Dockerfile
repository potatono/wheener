FROM ubuntu:latest

# Set up the basics
RUN apt-get update
RUN apt-get install -y build-essential
RUN apt-get install -y curl
RUN apt-get install -y ffmpeg
RUN apt-get install -y python3-pip

# Set up the work dir
RUN mkdir -p /usr/local/mediamtx
WORKDIR /usr/local/mediamtx
RUN mkdir -p bin log conf scripts

RUN pip3 install --break-system-packages requests firebase_admin flask

# Extract the latest arm64 binary from github (needs to be periodically updated)
RUN curl -sL -o - https://github.com/bluenviron/mediamtx/releases/download/v1.9.3/mediamtx_v1.9.3_linux_arm64v8.tar.gz | tar zvxf -
RUN mv mediamtx bin/mediamtx
# Move the default config to a name that won't be confusing.
# Moving to conf/ dir will hide it entirely when mounting conf/ as volume
RUN mv mediamtx.yml mediamtx.yml.sample

# Extract the latest cloudflared binary from github
#RUN curl -sL -o /usr/local/mediamtx/bin/cloudflared https://github.com/cloudflare/cloudflared/releases/download/2024.11.0/cloudflared-linux-arm64
#RUN chmod 0755 /usr/local/mediamtx/bin/cloudflared

ENTRYPOINT ["bin/mediamtx", "conf/mediamtx.yml"]
#ENTRYPOINT ["sleep","1000000000"]

