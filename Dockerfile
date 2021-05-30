FROM python:3.7.8-buster

RUN apt-get update -y 
#apt-get install -y python3-pip libpython3-dev iputils-ping

COPY ./requirements.txt /app/requirements.txt
WORKDIR /app

RUN pip3 install -r requirements.txt

COPY . /app

ENTRYPOINT ["python3"]
CMD ["run.py"]

