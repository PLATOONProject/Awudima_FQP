FROM ubuntu:20.04
MAINTAINER Kemele M. Endris <kemele.endris@gmail.com>

USER root

# Python 3.6 and Java 8 installation
RUN apt-get update && \
    apt-get install -y --no-install-recommends nano wget git curl less psmisc && \
    apt-get install -y --no-install-recommends python3.6 python3-pip python3-setuptools && \
    pip3 install --upgrade pip && \
    apt-get install -y --no-install-recommends openjdk-8-jre-headless && \
    apt-get clean

COPY . /Awudima_FQP
RUN cd /Awudima_FQP && pip3 install -r requirements.txt && \
    python3 setup.py install

RUN mkdir /data
WORKDIR /data
EXPOSE 8000
ENV CONFIG_FILE /data/federation.json
RUN chmod +x /Awudima_FQP/start_endpoint.sh

CMD ["/Awudima_FQP/start_endpoint.sh"]

# CMD ["tail", "-f", "/dev/null"]
