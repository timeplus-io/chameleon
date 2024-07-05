FROM golang:1.21.5 as builder
RUN mkdir /build
ADD . /build/
WORKDIR /build
RUN CGO_ENABLED=0 GOOS=linux go build -a -o ./cardemo ./main.go

FROM alpine:3.13.6
RUN apk update && apk add --no-cache curl
WORKDIR /timeplus
RUN curl -o /timeplus/routes.json https://tp-solutions.s3.us-west-2.amazonaws.com/nyc-trip-data/routes.json

COPY --from=builder /build/cardemo /timeplus/cardemo
RUN chmod +x /timeplus/cardemo

ENTRYPOINT [ "/timeplus/cardemo"]