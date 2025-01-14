#!/bin/sh

ffmpeg -i rtsp://localhost:8554/mystream \
    -pix_fmt yuvj420p \
    -x264-params keyint=48:min-keyint=48:scenecut=-1 \
    -b:v 4500k \
    -b:a 128k \
    -ar 44100 \
    -acodec aac \
    -vcodec libx264 \
    -preset medium \
    -crf 28 \
    -threads 4 \
    -f flv $STREAM_URL/$STREAM_KEY