FROM python:3.7

MAINTAINER Stefano Tavonatti <stefano.tavonatti@u-hopper.com>

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY run.sh .

ADD src/ /scripts/
ADD documentation/ /documentation/

ENV PYTHONPATH /scripts

CMD ["./run.sh"]
