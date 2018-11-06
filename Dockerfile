# Build frontend
#FROM clojure:alpine as frontend-builder
#ADD ./src/frontend /src/frontend
#RUN cd /src/frontend && \
#    lein deps && \
#    lein clean && \
#    lein package


FROM ubuntu:18.04 as backend-builder
ENV DEBIAN_FRONTEND noninteractive
ENV INITRD No
ADD ./src/backend/requirements.txt /requirements.txt
RUN apt-get update && \
    apt-get -y install git build-essential gcc make clang python3-dev python3-pip python3 libpcap-dev && \
    \
    # Build Python packages
    pip3 install --force-reinstall --upgrade -r /requirements.txt && \
    \
    # Clone and build masscan
    git clone https://github.com/robertdavidgraham/masscan && \
    cd masscan && \
    make -j && \
    mv bin/masscan /bin/masscan && \
    cd / && \
    rm -rf masscan && \
    \
    # Cleanup
    apt-get remove --purge -y git build-essential gcc make clang python3-dev python3-pip python3 libpcap-dev && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/


# Base
FROM ubuntu:18.04 as base
ENV DEBIAN_FRONTEND noninteractive
ENV INITRD No
ENV IPFS_PATH /home/ipfs-user/.ipfs

# Add a separate user for running IPFS node with custom iptables settings
RUN useradd -u 500 -ms /bin/bash ipfs-user

# Set up /dev/net device directories in order for Onioncat to work
RUN mkdir -p /dev/net && \
    mknod /dev/net/tun c 10 200

ADD ./scripts /scripts
RUN mkdir logs && chmod +x /scripts/docker-entrypoint.sh 
ADD ./default_config /default_config
ADD ./src/backend /src/backend
ADD ./assets /assets
COPY --from=backend-builder /usr/local/lib/python3.6/dist-packages /usr/local/lib/python3.6/dist-packages
COPY --from=backend-builder /usr/local/bin/circusd /usr/local/bin/
COPY --from=backend-builder /bin/masscan /bin/masscan
#COPY --from=frontend-builder /src/frontend/web /web

RUN apt-get update && \
    apt-get -y --no-install-recommends install python3 mongodb tor wget libpcap-dev onioncat net-tools kmod iptables && \
    \
    # Download go-IPFS release
    wget http://dist.ipfs.io/go-ipfs/v0.4.17/go-ipfs_v0.4.17_linux-amd64.tar.gz && \
    tar zxf go-ipfs_v0.4.17_linux-amd64.tar.gz && \
    cd go-ipfs && \
    mv ipfs /usr/bin/ipfs && \
    chmod +x /usr/bin/ipfs && \
    cd / && \
    rm -rf go-ipfs && rm go-ipfs_v0.4.17_linux-amd64.tar.gz && \
    \
    # Cleanup
    apt-get remove --purge -y wget && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/

CMD ["bash", "scripts/run.sh"]
