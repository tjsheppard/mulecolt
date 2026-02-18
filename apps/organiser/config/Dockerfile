FROM python:3.12-slim

# Install rclone + FUSE so the organiser can mount Zurg WebDAV directly
# (macOS Docker Desktop cannot propagate FUSE mounts between containers)
RUN apt-get update && \
    apt-get install -y --no-install-recommends fuse3 curl ca-certificates unzip && \
    curl https://rclone.org/install.sh | bash && \
    rm -rf /var/lib/apt/lists/* && \
    echo "user_allow_other" >> /etc/fuse.conf && \
    mkdir -p /zurg

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY organiser.py .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
