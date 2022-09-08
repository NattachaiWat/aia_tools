FROM python:3.9-slim-bullseye as baseline
RUN apt-get update && apt-get upgrade -y && apt-get clean

# try integrate
ENV TZ "Asia/Bangkok"
RUN apt-get -y install build-essential
RUN apt-get -y install libgl1-mesa-glx
RUN apt-get -y install libsm6 libxext6 git

RUN pip install --upgrade pip

WORKDIR /app
COPY requirements.txt /app
RUN pip install -r requirements.txt
COPY . /app