FROM python:3.7

RUN mkdir /eks

WORKDIR /eks

ADD . /eks/

RUN pip install -r requirements.txt

RUN pip install pandas

ENTRYPOINT  ["python", "/eks/Worker.py"]