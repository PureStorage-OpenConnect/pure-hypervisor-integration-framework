"""Manages the Everpure vSphere Client Plugin Docker containers from within PHIF.

The plugin consists of two containers that run alongside the PHIF stack:
  vsphere-plugin-server   — everpure/client-plugin-vsphere (port 8080 internal)
  vsphere-plugin-proxy    — everpure/client-plugin-vsphere-reverse-proxy (port 9443)

Both are published on Docker Hub, so the default install path is simply to pull
them. Loading images from a local tar remains supported for air-gapped sites and
for images mirrored into a private registry.

This manager drives them via the Docker socket mounted into the backend container.
It self-inspects its own container mounts to find the correct host-side cert path
so volume binds work correctly without any extra env vars.

Prerequisites (satisfied by docker-compose.yml):
  - /var/run/docker.sock mounted into the backend container
  - /certs mounted read-write (so the plugin secret can be generated there)
"""
from __future__ import annotations

import asyncio
import base64
import logging
import os
import secrets
import socket
import threading
from pathlib import Path
from typing import Any

from phif.config import get_settings

log = logging.getLogger(__name__)

PLUGIN_CONTAINER = "phif-vsphere-plugin-server-1"
PROXY_CONTAINER = "phif-vsphere-plugin-proxy-1"
STORAGE_VOLUME = "phif_vsphere-plugin-storage"

# Repository names the plugin image may carry besides the configured one. An
# image loaded from a tar built before the Docker Hub publish, or mirrored into
# a private registry, keeps its original repository name — accept those too so
# an existing offline install is not invalidated by the new default.
PLUGIN_REPO_ALIASES = (
    "everpure/client-plugin-vsphere",
    "purestorage/vsphere-plugin",
    "vsphere-plugin",
)
PROXY_REPO_ALIASES = (
    "everpure/client-plugin-vsphere-reverse-proxy",
    "purestorage/vsphere-plugin-reverse-proxy",
    "vsphere-plugin-reverse-proxy",
)

# Where /certs is mounted inside the backend container.
_CERTS_DEST = "/certs"


def plugin_image() -> str:
    """Configured plugin server image reference (repo:tag)."""
    return get_settings().vsphere_plugin_image


def proxy_image() -> str:
    """Configured reverse-proxy image reference (repo:tag)."""
    return get_settings().vsphere_plugin_proxy_image


def _split_ref(ref: str) -> tuple[str, str]:
    """Split ``repo:tag`` into its parts, defaulting the tag to ``latest``.

    Only the portion after the last ``/`` may contain the tag separator, so a
    registry host with a port (``registry.example.com:5000/foo``) is not
    mistaken for a tag.
    """
    head, sep, tail = ref.rpartition(":")
    if sep and "/" not in tail:
        return head, tail
    return ref, "latest"


def _plugin_repos() -> tuple[str, ...]:
    """Repo names accepted as the plugin server image, configured one first."""
    repo = _split_ref(plugin_image())[0]
    return (repo, *(r for r in PLUGIN_REPO_ALIASES if r != repo))


def _proxy_repos() -> tuple[str, ...]:
    """Repo names accepted as the reverse-proxy image, configured one first."""
    repo = _split_ref(proxy_image())[0]
    return (repo, *(r for r in PROXY_REPO_ALIASES if r != repo))


class VspherePluginManager:
    """Manages vsphere plugin containers via the host Docker socket."""

    def __init__(self) -> None:
        self._client = None
        # Progress of a detached pull/install, published through status().
        self._install_lock = threading.Lock()
        self._install: dict[str, Any] = {
            "active": False, "phase": "", "detail": "", "percent": None,
            "image": None, "error": None, "done": False,
        }

    # ---------------------------------------------------------------- docker --

    def _docker(self):
        if self._client is None:
            import docker  # lazy: docker SDK optional outside vsphere use
            self._client = docker.from_env()
        return self._client

    def _self_id(self) -> str:
        return socket.gethostname()

    def _host_certs_path(self) -> str:
        """Return the HOST-side path of the /certs bind mount.

        The backend container has /certs bind-mounted from the project's certs/
        directory. We inspect our own container to find the source so we can
        pass the correct host path to the vsphere plugin container's volume binds.
        Falls back to /certs when running outside Docker (dev mode).
        """
        try:
            me = self._docker().containers.get(self._self_id())
            for m in me.attrs.get("Mounts", []):
                if m.get("Destination") == _CERTS_DEST and m.get("Source"):
                    return m["Source"]
        except Exception as exc:
            log.warning("Cannot inspect own container mounts: %s", exc)
        return _CERTS_DEST

    def _compose_project(self) -> str:
        """Return the compose project name from our container's labels."""
        try:
            me = self._docker().containers.get(self._self_id())
            return me.labels.get("com.docker.compose.project", "phif")
        except Exception:
            return "phif"

    def _network(self) -> str:
        return f"{self._compose_project()}_default"

    def _get_container(self, name: str):
        try:
            return self._docker().containers.get(name)
        except Exception:
            return None

    def _container_status(self, name: str) -> str:
        c = self._get_container(name)
        if c is None:
            return "missing"
        return c.status  # "running" | "exited" | "created" | …

    @staticmethod
    def _tag_matches_repo(tag: str, repo: str) -> bool:
        """Return True if `tag` refers to exactly `repo` (not a longer name sharing a prefix).

        Full registry tags look like registry.example.com/path/client-plugin-vsphere:dev.
        We strip the version suffix and check that the repository portion ends with the exact
        repo name preceded by '/' or the tag IS exactly the repo (no prefix).

        This prevents "client-plugin-vsphere" from matching
        "client-plugin-vsphere-reverse-proxy".
        """
        tag_repo = _split_ref(tag)[0]
        return tag_repo == repo or tag_repo.endswith(f"/{repo}")

    def _find_local_image(self, repos: tuple[str, ...]):
        """Return the first local image tagged with any repo in `repos`, else None.

        `repos` is ordered most-preferred first, so the configured image wins
        over a legacy alias when both happen to be present.
        """
        try:
            images = self._docker().images.list()
        except Exception:
            return None
        for repo in repos:
            for img in images:
                if any(self._tag_matches_repo(t, repo) for t in (img.tags or [])):
                    return img
        return None

    def _image_exists(self, repos: tuple[str, ...]) -> bool:
        """Return True if any local image matches one of `repos`."""
        return self._find_local_image(repos) is not None

    def _ensure_image_ref(self, repos: tuple[str, ...], ref: str) -> None:
        """Make `ref` resolvable locally, re-tagging an alias-matched image if needed.

        A pulled image already carries the configured ref. An image loaded from a
        tar may instead carry a legacy repo name or a full private-registry path
        (e.g. registry.example.com/.../client-plugin-vsphere:dev), which Docker
        will not find by the configured short name — so re-tag it before starting.
        """
        try:
            self._docker().images.get(ref)
            return  # already resolvable
        except Exception:
            pass
        img = self._find_local_image(repos)
        if img is None:
            raise RuntimeError(
                f"No local image found for '{ref}'. Pull it from Docker Hub or "
                f"upload the image tar first.")
        repo, tag = _split_ref(ref)
        img.tag(repo, tag=tag)
        log.info("Re-tagged image %s → %s", img.tags, ref)

    # -------------------------------------------------------------- secret --

    def _secret_path(self) -> Path:
        return Path(_CERTS_DEST) / "vsphere-plugin-secret"

    def ensure_secret(self) -> None:
        """Generate the plugin secret file if it does not exist."""
        path = self._secret_path()
        if not path.exists():
            secret = base64.b64encode(secrets.token_bytes(96)).decode()
            path.write_text(secret)
            log.info("Generated vsphere plugin secret at %s", path)

    # ------------------------------------------------------------ status --

    def status(self) -> dict[str, Any]:
        """Return the current state of the vsphere plugin containers and images."""
        try:
            server_st = self._container_status(PLUGIN_CONTAINER)
            proxy_st = self._container_status(PROXY_CONTAINER)
            server_img = self._image_exists(_plugin_repos())
            proxy_img = self._image_exists(_proxy_repos())
            secret_ok = self._secret_path().exists()
            return {
                "server": server_st,
                "proxy": proxy_st,
                "images": {"server": server_img, "proxy": proxy_img},
                "image_refs": {"server": plugin_image(), "proxy": proxy_image()},
                "secret_exists": secret_ok,
                "can_start": server_img and proxy_img,
                "available": True,
                "install": self.install_progress(),
            }
        except Exception as exc:
            return {"available": False, "error": str(exc)}

    # ------------------------------------------------------- image pull --

    def install_progress(self) -> dict[str, Any]:
        """Snapshot of any in-flight pull/install, safe to serialise."""
        with self._install_lock:
            return dict(self._install)

    def _set_install(self, **fields: Any) -> None:
        with self._install_lock:
            self._install.update(fields)

    def pull_images(self, *, start_after: bool = False) -> None:
        """Pull both plugin images from the configured registry (Docker Hub by default).

        Runs synchronously; callers that need it in the background use
        :func:`start_install`. When `start_after` is set, the containers are
        started as soon as both images are present, making this the one-step
        default install path.
        """
        api = self._docker().api
        for label, ref in (("server", plugin_image()), ("proxy", proxy_image())):
            repo, tag = _split_ref(ref)
            self._set_install(phase=f"Pulling {ref}", detail="", image=label)
            log.info("Pulling vsphere plugin image %s", ref)
            layers: dict[str, tuple[int, int]] = {}
            for event in api.pull(repo, tag=tag, stream=True, decode=True):
                if "error" in event:
                    raise RuntimeError(f"Pulling {ref} failed: {event['error']}")
                # Aggregate per-layer byte counts into one overall percentage.
                lid = event.get("id")
                prog = event.get("progressDetail") or {}
                if lid and prog.get("total"):
                    layers[lid] = (prog.get("current", 0), prog["total"])
                current = sum(c for c, _ in layers.values())
                total = sum(t for _, t in layers.values())
                pct = int(current * 100 / total) if total else None
                self._set_install(detail=event.get("status") or "", percent=pct)
            log.info("Pulled %s", ref)
        self._set_install(phase="Images ready", detail="", percent=None)

        if start_after:
            self._set_install(phase="Starting containers", detail="")
            self.ensure_secret()
            self.start()
            self._set_install(phase="Running", detail="")

    def start_install(self, *, pull: bool = True, start_after: bool = True) -> dict[str, Any]:
        """Kick off a pull (and optional start) on a worker thread.

        Pulling ~400 MB of images far outlasts a normal HTTP request, so the
        work runs detached and the UI follows it through ``status()``. Returns
        the initial progress snapshot. Raises if an install is already running.
        """
        with self._install_lock:
            if self._install.get("active"):
                raise RuntimeError("An install is already in progress")
            self._install = {
                "active": True, "phase": "Starting", "detail": "",
                "percent": None, "image": None, "error": None, "done": False,
            }

        def _worker() -> None:
            try:
                if pull:
                    self.pull_images(start_after=start_after)
                elif start_after:
                    self._set_install(phase="Starting containers")
                    self.ensure_secret()
                    self.start()
                    self._set_install(phase="Running")
                self._set_install(active=False, done=True, percent=None)
            except Exception as exc:
                log.exception("vsphere plugin install failed")
                self._set_install(active=False, done=True, error=str(exc),
                                  phase="Failed", percent=None)

        threading.Thread(target=_worker, name="vsphere-plugin-install",
                         daemon=True).start()
        return self.install_progress()

    # -------------------------------------------------------- image load --

    def load_image_from_file(self, tar_path: str) -> list[str]:
        """Load a Docker image from a tar file; return the loaded image tags.

        Re-tags each loaded image to the configured image reference if it does
        not already carry it, so start() can use one stable name regardless of
        how the tar was built.
        """
        with open(tar_path, "rb") as f:
            images = self._docker().images.load(f)
        tags = [t for img in images for t in (img.tags or [])]
        log.info("Loaded image(s) from %s: %s", tar_path, tags)

        # A tar may carry a legacy repo name or a full private-registry path
        # (registry.example.com/.../client-plugin-vsphere:dev). Match on the
        # exact repo path suffix — never a prefix — so "client-plugin-vsphere"
        # does not swallow "client-plugin-vsphere-reverse-proxy".
        for img in images:
            for repos, ref in (
                (_plugin_repos(), plugin_image()),
                (_proxy_repos(), proxy_image()),
            ):
                img_tags = img.tags or []
                if ref in img_tags:
                    continue
                if any(self._tag_matches_repo(t, r) for t in img_tags for r in repos):
                    repo, tag = _split_ref(ref)
                    img.tag(repo, tag=tag)
                    log.info("Re-tagged %s → %s", img_tags, ref)
                    tags.append(ref)

        return tags

    # --------------------------------------------------------- lifecycle --

    def start(self) -> None:
        """Start (or restart) the vsphere plugin server and proxy containers."""
        # Make both configured refs resolvable locally so containers.run() never
        # has to reach the registry itself (an offline install must still work).
        plugin_ref, proxy_ref = plugin_image(), proxy_image()
        self._ensure_image_ref(_plugin_repos(), plugin_ref)
        self._ensure_image_ref(_proxy_repos(), proxy_ref)

        host_certs = self._host_certs_path()

        # The proxy nginx process runs as a non-root user and needs to read the
        # TLS key. Keys generated by gen-certs.sh default to 0600 (owner-only).
        # Make it group-readable so the bind-mount is accessible in the container.
        key_path = Path(_CERTS_DEST) / "tls.key"
        if key_path.exists() and not (key_path.stat().st_mode & 0o044):
            os.chmod(key_path, key_path.stat().st_mode | 0o044)
            log.info("Made %s group/world-readable for proxy container", key_path)
        network = self._network()
        project = self._compose_project()

        # Ensure the named storage volume exists.
        try:
            self._docker().volumes.get(STORAGE_VOLUME)
        except Exception:
            self._docker().volumes.create(STORAGE_VOLUME)
            log.info("Created Docker volume %s", STORAGE_VOLUME)

        # --- plugin server ---
        existing = self._get_container(PLUGIN_CONTAINER)
        if existing is not None:
            if existing.status != "running":
                existing.start()
                log.info("Started existing container %s", PLUGIN_CONTAINER)
        else:
            self._docker().containers.run(
                plugin_ref,
                name=PLUGIN_CONTAINER,
                detach=True,
                read_only=True,
                security_opt=["no-new-privileges:true"],
                tmpfs={"/tmp": ""},
                environment={
                    "CERTIFICATE_PATH": "/run/secrets/vsphere_cert",
                    "SECRET_KEY_PATH": "/run/secrets/vsphere_secret_key",
                },
                volumes={
                    f"{host_certs}/tls.crt": {
                        "bind": "/run/secrets/vsphere_cert", "mode": "ro"},
                    f"{host_certs}/vsphere-plugin-secret": {
                        "bind": "/run/secrets/vsphere_secret_key", "mode": "ro"},
                    STORAGE_VOLUME: {"bind": "/storage", "mode": "rw"},
                },
                network=network,
                restart_policy={"Name": "unless-stopped"},
                labels={
                    "com.docker.compose.project": project,
                    "com.docker.compose.service": "vsphere-plugin-server",
                    "phif.managed": "true",
                },
            )
            log.info("Started container %s", PLUGIN_CONTAINER)

        # --- proxy ---
        existing_proxy = self._get_container(PROXY_CONTAINER)
        if existing_proxy is not None:
            if existing_proxy.status != "running":
                existing_proxy.start()
                log.info("Started existing container %s", PROXY_CONTAINER)
        else:
            self._docker().containers.run(
                proxy_ref,
                name=PROXY_CONTAINER,
                detach=True,
                read_only=True,
                security_opt=["no-new-privileges:true"],
                tmpfs={"/tmp": "", "/tmpconf": "mode=1777,size=24k"},
                environment={
                    "CERTIFICATE_PATH": "/run/secrets/vsphere_cert",
                    "CERTIFICATE_KEY_PATH": "/run/secrets/vsphere_key",
                    "VSPHERE_PLUGIN_SERVICE": f"{PLUGIN_CONTAINER}:8080",
                    "NGINX_ENVSUBST_OUTPUT_DIR": "/tmpconf",
                },
                volumes={
                    f"{host_certs}/tls.crt": {
                        "bind": "/run/secrets/vsphere_cert", "mode": "ro"},
                    f"{host_certs}/tls.key": {
                        "bind": "/run/secrets/vsphere_key", "mode": "ro"},
                },
                ports={"8084/tcp": 9443, "8080/tcp": 9081},
                network=network,
                restart_policy={"Name": "unless-stopped"},
                labels={
                    "com.docker.compose.project": project,
                    "com.docker.compose.service": "vsphere-plugin-proxy",
                    "phif.managed": "true",
                },
            )
            log.info("Started container %s", PROXY_CONTAINER)

    def stop(self) -> None:
        """Stop and remove the vsphere plugin containers."""
        for name in (PROXY_CONTAINER, PLUGIN_CONTAINER):
            c = self._get_container(name)
            if c is not None:
                try:
                    c.stop(timeout=10)
                    c.remove()
                    log.info("Stopped and removed %s", name)
                except Exception as exc:
                    log.warning("Error stopping %s: %s", name, exc)


# Module-level singleton so the Docker client is reused across requests.
_manager: VspherePluginManager | None = None


def get_manager() -> VspherePluginManager:
    global _manager
    if _manager is None:
        _manager = VspherePluginManager()
    return _manager


async def async_status() -> dict[str, Any]:
    return await asyncio.to_thread(get_manager().status)


async def async_load_image(tar_path: str) -> list[str]:
    return await asyncio.to_thread(get_manager().load_image_from_file, tar_path)


async def async_start_install(*, pull: bool = True,
                              start_after: bool = True) -> dict[str, Any]:
    """Begin a detached pull/start; returns at once with the initial progress."""
    return await asyncio.to_thread(
        lambda: get_manager().start_install(pull=pull, start_after=start_after))


async def async_start() -> None:
    get_manager().ensure_secret()
    await asyncio.to_thread(get_manager().start)


async def async_stop() -> None:
    await asyncio.to_thread(get_manager().stop)
