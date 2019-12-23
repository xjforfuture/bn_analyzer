FROM cdqhyx.cn:8888/base/bn_analyzer:release
MAINTAINER qhyx:tsinghuanet.com

COPY ./analyzer /home/analyzer
COPY ./config /home/config
COPY ./data /home/data

RUN pip3 install /home/analyzer \
    && mkdir /home/log

WORKDIR /home

CMD ["/usr/bin/supervisord", "-c", "/home/config/supervisord.conf"]

