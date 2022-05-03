FROM golang:1.17.1-alpine3.13 as builder
RUN mkdir /build
ADD . /build/
WORKDIR /build
RUN CGO_ENABLED=0 GOOS=linux go build -a -o ./generator ./main.go

FROM alpine:3.13.6
WORKDIR /timeplus
COPY --from=builder /build/generator /timeplus/generator
RUN chmod +x /timeplus/generator

EXPOSE 3000

ENTRYPOINT [ "/timeplus/generator" ]