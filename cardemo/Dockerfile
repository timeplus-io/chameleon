FROM golang:1.18.3 as builder
RUN mkdir /build
ADD . /build/
WORKDIR /build
RUN CGO_ENABLED=0 GOOS=linux go build -a -o ./cardemo ./main.go

FROM alpine:3.13.6
WORKDIR /timeplus
COPY --from=builder /build/cardemo /timeplus/cardemo
COPY ./sinks.yaml /timeplus/config/
RUN chmod +x /timeplus/cardemo

ENTRYPOINT [ "/timeplus/cardemo"]