FROM golang:stretch as build

RUN go get github.com/Masterminds/glide && \
    go get github.com/alecthomas/gometalinter && \
    go get github.com/tinylib/msgp
    
RUN apt-get update && apt-get install -y netcat

RUN mkdir -p $GOPATH/src/github.com/uber && \
    cd $GOPATH/src/github.com/uber && \
    git clone https://github.com/uber/storagetapper.git && \
    cd storagetapper && \
    rm -f pipe/hdfs.go && \
    DEB_BUILD_OPTIONS=nocheck make install

ADD wait_for.sh /wait_for.sh
RUN chmod +x /wait_for.sh

VOLUME ['/etc/storagetapper/']
ENTRYPOINT ['/wait_for.sh', 'storagetapper']