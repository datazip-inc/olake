FROM golang:1.23-alpine as base

ADD . /home/app/
# ADD . /home/app/
WORKDIR /home/app/drivers/mongodb/

RUN gofmt -l -s -w .
RUN go build -o dsynk main.go
RUN mv dsynk /
# RUN mv generated.json /

FROM golang:1.21-alpine
COPY --from=base /dsynk /home/
# COPY --from=base /generated.json /home/generated.json
ADD . /home

LABEL io.eggwhite.version=2.0.24
LABEL io.eggwhite.name=airbyte/source-mysql

WORKDIR /home
ENTRYPOINT [ "./dsynk" ]