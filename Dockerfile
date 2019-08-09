FROM golang:1.12-alpine as build

RUN apk add --update git gcc musl-dev

RUN mkdir /app

WORKDIR /app

COPY go.sum go.mod ./

RUN go mod download

COPY . .

RUN go build -a -tags netgo -installsuffix netgo --ldflags '-extldflags "-static"' -o server

FROM scratch

COPY --from=build /app/server /bin/

ENV PORT=80
ENV ARCHIVE_PATH=/tmp/data/data.zip

CMD [ "/bin/server" ]