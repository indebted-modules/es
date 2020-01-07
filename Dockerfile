FROM golang:1.13.4
RUN go get -u golang.org/x/lint/golint
RUN go get -u golang.org/x/tools/cmd/goimports
RUN go get -u github.com/tj/mmake/cmd/mmake
WORKDIR /src
