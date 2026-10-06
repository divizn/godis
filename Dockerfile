FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/godis ./cmd/server && mkdir /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/godis /godis
COPY --from=build --chown=nonroot:nonroot /out/data /data
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/godis", "--data-dir=/data"]
