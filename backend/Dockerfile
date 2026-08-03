# PHIF backend image. Bundles the API plus the CLIs/SDKs connectors shell out to
# (Ansible + purestorage collection, kubectl, helm). The OpenStack client is
# pip-installed via the connector's optional deps when needed.
#
# Build context is the REPO ROOT (so the top-level ansible/ tree is available);
# build with: docker build -f backend/Dockerfile .  (compose sets this up).

# ---------------------------------------------------------------------------
# Stage: build the HPE VME (Morpheus) Everpure storage plugin JAR.
# Compiles the Groovy plugin against the real morpheus-plugin-api (Maven Central)
# and produces the shadow ("fat") JAR the hpevme connector uploads to VME. This
# also serves as the real-SDK compile check — if the plugin doesn't compile, the
# image build fails here instead of silently shipping no JAR.
# ---------------------------------------------------------------------------
FROM gradle:8.5-jdk11 AS plugin-build
WORKDIR /home/gradle/plugin
COPY --chown=gradle:gradle backend/phif/connectors/hpevme/files/morpheus-plugin/ ./
RUN gradle --no-daemon shadowJar
# -> /home/gradle/plugin/build/libs/pure-flasharray-vme-plugin-<ver>-all.jar

FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates openssh-client git \
    && rm -rf /var/lib/apt/lists/*

# kubectl + helm for the OpenShift/Kubernetes connector.
RUN curl -fsSL https://dl.k8s.io/release/v1.30.0/bin/linux/amd64/kubectl -o /usr/local/bin/kubectl \
    && chmod +x /usr/local/bin/kubectl \
    && curl -fsSL https://get.helm.sh/helm-v3.15.2-linux-amd64.tar.gz | tar xz \
    && mv linux-amd64/helm /usr/local/bin/helm && rm -rf linux-amd64

# oc (OpenShift CLI) for the OpenShift connector. Required for the operations
# kubectl can't do: `oc login` (OAuth password grant), `oc adm policy`,
# `oc create token`, and the MachineConfig CRs. The client tarball bundles oc
# (+ a kubectl); we keep only oc since kubectl is installed above.
RUN curl -fsSL https://mirror.openshift.com/pub/openshift-v4/x86_64/clients/ocp/stable/openshift-client-linux.tar.gz \
        | tar xz -C /usr/local/bin oc \
    && chmod +x /usr/local/bin/oc

WORKDIR /app
COPY backend/pyproject.toml ./
RUN pip install --upgrade pip && pip install ".[dev]"

# Everpure's official Ansible collection used by several connectors.
RUN ansible-galaxy collection install purestorage.flasharray || true

COPY backend/phif ./phif
COPY ansible /app/ansible

# Drop the prebuilt HPE VME plugin JAR where the hpevme connector's deploy action
# looks for it (files/morpheus-plugin/build/libs/...-all.jar), so `deploy` can
# upload it to the VME Manager with no manual gradle step.
COPY --from=plugin-build /home/gradle/plugin/build/libs/*-all.jar \
    /app/phif/connectors/hpevme/files/morpheus-plugin/build/libs/

EXPOSE 8000
CMD ["uvicorn", "phif.main:app", "--host", "0.0.0.0", "--port", "8000"]
