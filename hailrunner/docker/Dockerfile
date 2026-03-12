FROM python:3.11-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        gnupg \
        apt-transport-https \
        ca-certificates \
        openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*
# Google Cloud SDK (tarball install — more reliable than the apt repo)
ARG GCLOUD_VERSION=516.0.0
RUN curl -fsSL "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-${GCLOUD_VERSION}-linux-x86_64.tar.gz" \
        | tar -xz -C /opt && \
    /opt/google-cloud-sdk/install.sh --quiet --path-update=false && \
    ln -s /opt/google-cloud-sdk/bin/gcloud /usr/local/bin/gcloud && \
    ln -s /opt/google-cloud-sdk/bin/gsutil /usr/local/bin/gsutil

# Pre-download Hail wheel + all dependencies for offline install on Dataproc
ARG HAIL_VERSION=0.2.132
RUN pip download hail==${HAIL_VERSION} -d /usr/local/share/hailrunner/wheels/

# hailrunner
COPY init_hail.sh /usr/local/share/hailrunner/init_hail.sh
COPY hailrunner.py /usr/local/bin/hailrunner
RUN chmod +x /usr/local/bin/hailrunner
ENTRYPOINT ["python3", "/usr/local/bin/hailrunner"]
