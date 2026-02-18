FROM alpine:3.19

ARG PB_VERSION=0.25.6

RUN apk add --no-cache ca-certificates curl unzip && \
    curl -fsSL -o /tmp/pb.zip \
      "https://github.com/pocketbase/pocketbase/releases/download/v${PB_VERSION}/pocketbase_${PB_VERSION}_linux_amd64.zip" && \
    unzip /tmp/pb.zip -d /usr/local/bin/ && \
    rm /tmp/pb.zip && \
    chmod +x /usr/local/bin/pocketbase && \
    mkdir -p /pb/pb_data /pb/pb_migrations

WORKDIR /pb

COPY pb_migrations/ /pb/pb_migrations/
COPY entrypoint.sh /pb/entrypoint.sh
RUN chmod +x /pb/entrypoint.sh

EXPOSE 8090

ENTRYPOINT ["/pb/entrypoint.sh"]
