FROM golang:1.17.1-alpine3.13 as builder
RUN mkdir /build
ADD . /build/
WORKDIR /build
RUN CGO_ENABLED=0 GOOS=linux go build -a -o ./tsbs_loader ./main.go

FROM alpine:3.13.6
WORKDIR /timeplus
COPY --from=builder /build/tsbs_loader /timeplus/tsbs_loader
COPY ./data/ /timeplus/data/
RUN chmod +x /timeplus/tsbs_loader

EXPOSE 3000

ENTRYPOINT [ "/timeplus/tsbs_loader" ]