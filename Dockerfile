FROM golang:1.3

MAINTAINER The OpenTarock Project

RUN mkdir /build && \
    curl -O http://download.nanomsg.org/nanomsg-0.4-beta.tar.gz && \
    { sum=`sha1sum nanomsg-0.4-beta.tar.gz`; if [ "$sum" != "a511f19f8574875e8e43f7ba27f7951f67fbe161  nanomsg-0.4-beta.tar.gz" ]; then exit 1; fi; } && \
    tar xvf nanomsg-0.4-beta.tar.gz && \
    cd nanomsg-0.4-beta && \
    ./configure && \
    make && \
    make check && \
    make install && \
    ldconfig

RUN go get github.com/tools/godep

ADD . /go/src/github.com/opentarock/service-presence

RUN cd /go/src/github.com/opentarock/service-presence && godep go install .

ENTRYPOINT /go/bin/service-presence

EXPOSE 9001
