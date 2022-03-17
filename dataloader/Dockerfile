FROM golang:1.17.1-alpine3.13 as builder
RUN mkdir /build
ADD . /build/
WORKDIR /build
RUN CGO_ENABLED=0 GOOS=linux go build cmd/dataloader.go

FROM alpine:3.13.6
RUN mkdir /timeplus
WORKDIR /timeplus
COPY --from=builder /build/dataloader /timeplus/dataloader
COPY --from=builder /build/config/config.yml /timeplus/config/config.yml

# executable
# ENTRYPOINT [ "/timeplus/dataloader" ]
CMD [ "/timeplus/dataloader" , "--config=config/config.yml"]