FROM golang:1.17.1-alpine3.13 as builder
RUN mkdir /build
ADD . /build/
WORKDIR /build
RUN CGO_ENABLED=0 GOOS=linux go build -a -o ./server ./main.go

FROM alpine:3.13.6
COPY --from=builder /build/server ./server

EXPOSE 3000

ENTRYPOINT [ "./server" ]