"""vSphere plugin container management API.

Provides endpoints to upload plugin images, start/stop the vsphere plugin
containers, and check their status — all from within the PHIF UI without
manual SSH or docker commands on the host.

The default install path pulls both images from Docker Hub. Uploading image
tars stays available for air-gapped sites.

Endpoints
---------
GET  /vsphere-plugin/status          — container + image state, install progress
POST /vsphere-plugin/install         — pull from Docker Hub, then start (default path)
POST /vsphere-plugin/pull            — pull images from Docker Hub only
POST /vsphere-plugin/images          — upload plugin/proxy tar files + docker load
POST /vsphere-plugin/start           — generate secret if needed, start containers
POST /vsphere-plugin/stop            — stop and remove containers

``install`` and ``pull`` return 202 immediately and run detached — pulling
several hundred MB outlasts any sane request timeout — so poll ``status`` for
progress.
"""
from __future__ import annotations

import logging
import os
import tempfile
from typing import Any

from fastapi import APIRouter, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from phif.vsphere_plugin.manager import (
    async_load_image,
    async_start,
    async_start_install,
    async_status,
    async_stop,
)

log = logging.getLogger(__name__)
router = APIRouter(prefix="/vsphere-plugin", tags=["vsphere-plugin"])

# Maximum size accepted for image tar uploads (600 MiB).
_MAX_UPLOAD_BYTES = 600 * 1024 * 1024


@router.get("/status")
async def get_status() -> dict[str, Any]:
    """Return the current state of vsphere plugin containers and images."""
    return await async_status()


@router.post("/images")
async def upload_images(
    plugin_tar: UploadFile | None = None,
    proxy_tar: UploadFile | None = None,
) -> dict[str, Any]:
    """Load vsphere plugin Docker images from uploaded tar files.

    Accepts ``plugin_tar`` (vsphere-plugin image) and/or ``proxy_tar``
    (vsphere-plugin-reverse-proxy image) as multipart file uploads. Each file
    is streamed to a temporary directory and loaded into the Docker daemon via
    the socket. Returns the tags of all loaded images.
    """
    if plugin_tar is None and proxy_tar is None:
        raise HTTPException(400, "Provide plugin_tar and/or proxy_tar")

    loaded: list[str] = []
    errors: list[str] = []

    _CHUNK = 4 * 1024 * 1024  # 4 MiB read chunks
    for label, upload in (("plugin", plugin_tar), ("proxy", proxy_tar)):
        if upload is None:
            continue
        suffix = os.path.splitext(upload.filename or "")[1] or ".tar"
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        try:
            written = 0
            while True:
                chunk = await upload.read(_CHUNK)
                if not chunk:
                    break
                written += len(chunk)
                if written > _MAX_UPLOAD_BYTES:
                    tmp.close()
                    os.unlink(tmp.name)
                    raise HTTPException(
                        413, f"{label} tar exceeds the {_MAX_UPLOAD_BYTES // (1024**2)} MiB limit")
                tmp.write(chunk)
            tmp.close()
            tags = await async_load_image(tmp.name)
            loaded.extend(tags)
            log.info("Loaded %s image(s) from %s upload: %s", label, upload.filename, tags)
        except HTTPException:
            raise
        except Exception as exc:
            log.exception("Failed to load %s image", label)
            errors.append(f"{label}: {exc}")
        finally:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass

    if errors and not loaded:
        raise HTTPException(500, "; ".join(errors))

    return {"loaded": loaded, "errors": errors}


async def _begin_install(*, pull: bool, start_after: bool) -> JSONResponse:
    """Kick off a detached pull/start and return 202 with the initial progress."""
    status = await async_status()
    if not status.get("available"):
        raise HTTPException(503, f"Docker unavailable: {status.get('error', 'unknown')}")
    try:
        progress = await async_start_install(pull=pull, start_after=start_after)
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc
    return JSONResponse(status_code=202, content={"install": progress})


@router.post("/install")
async def install_plugin() -> JSONResponse:
    """Default install: pull both images from Docker Hub, then start the containers.

    Returns 202 immediately — the pull runs detached and its progress is
    reported by ``GET /vsphere-plugin/status`` under ``install``.
    """
    return await _begin_install(pull=True, start_after=True)


@router.post("/pull")
async def pull_images() -> JSONResponse:
    """Pull both plugin images from Docker Hub without starting the containers."""
    return await _begin_install(pull=True, start_after=False)


@router.post("/start")
async def start_plugin() -> dict[str, Any]:
    """Generate the plugin secret (if needed) and start the vsphere plugin containers."""
    status = await async_status()
    if not status.get("available"):
        raise HTTPException(503, f"Docker unavailable: {status.get('error', 'unknown')}")
    images = status.get("images", {})
    if not images.get("server") or not images.get("proxy"):
        missing = [k for k, v in images.items() if not v]
        raise HTTPException(
            400,
            f"Missing images: {', '.join(missing)}. Install them from Docker Hub "
            f"or upload the image tars first.")
    try:
        await async_start()
    except Exception as exc:
        log.exception("Failed to start vsphere plugin containers")
        raise HTTPException(500, str(exc))
    return await async_status()


@router.post("/stop")
async def stop_plugin() -> dict[str, Any]:
    """Stop and remove the vsphere plugin containers."""
    try:
        await async_stop()
    except Exception as exc:
        log.exception("Failed to stop vsphere plugin containers")
        raise HTTPException(500, str(exc))
    return await async_status()
