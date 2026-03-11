FROM python:3.11-slim
ARG HAIL_VERSION=0.2.132
ENV HAIL_VERSION=$HAIL_VERSION
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
# Hail (includes hailctl)
RUN pip install --no-cache-dir hail==${HAIL_VERSION}
# hailrunner
COPY hailrunner.py /usr/local/bin/hailrunner
RUN chmod +x /usr/local/bin/hailrunner
ENTRYPOINT ["python3", "/usr/local/bin/hailrunner"]
