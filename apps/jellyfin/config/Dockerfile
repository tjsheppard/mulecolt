FROM linuxserver/jellyfin:latest

# Install rclone + FUSE so Jellyfin can mount Zurg WebDAV directly
# (macOS Docker Desktop cannot propagate FUSE mounts between containers)
RUN apt-get update && \
    apt-get install -y --no-install-recommends fuse3 unzip && \
    curl https://rclone.org/install.sh | bash && \
    rm -rf /var/lib/apt/lists/* && \
    echo "user_allow_other" >> /etc/fuse.conf && \
    mkdir -p /zurg

# Register rclone mount as a persistent s6 service
COPY rclone-service/ /etc/s6-overlay/s6-rc.d/svc-rclone/
RUN touch /etc/s6-overlay/s6-rc.d/user/contents.d/svc-rclone
