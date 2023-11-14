FROM python:3.10-slim-bullseye

WORKDIR /python-docker
COPY oracle-instantclient-basic_21.11.0.0.0-2_amd64.deb .
RUN apt-get update && apt-get install libaio1 libaio-dev
RUN dpkg -i oracle-instantclient-basic_21.11.0.0.0-2_amd64.deb
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY . .

CMD [ "python3", "main.py"]

