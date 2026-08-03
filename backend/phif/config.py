"""Application configuration, loaded from environment / .env."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PHIF_", env_file=".env", extra="ignore")

    # Database
    # The phif:phif credential is a local-dev/quickstart default only — Postgres
    # is never port-published in docker-compose.yml. Override PHIF_DATABASE_URL
    # for any shared or production deployment.
    database_url: str = "postgresql+psycopg://phif:phif@localhost:5432/phif"

    # Vault master key (Fernet key, base64 urlsafe 32 bytes). MUST be set in prod.
    # If unset, a deterministic dev key is derived (insecure — dev only).
    vault_master_key: str = ""

    # API / CORS
    api_prefix: str = "/api"
    cors_origins: list[str] = ["http://localhost:5173", "http://localhost:3000"]

    # Job engine
    job_workspace: str = "/tmp/phif-jobs"
    max_concurrent_jobs: int = 8

    # Ansible
    ansible_collections_path: str = ""  # extra collections path if pre-installed

    # vSphere Client Plugin container images, on public Docker Hub so a fresh
    # install can pull them with no manual image handling. Override to point at
    # a private registry mirror or to move to a newer plugin release.
    #
    # The tag is pinned deliberately: neither repository publishes a `latest`
    # tag, so an unpinned reference fails to resolve ("no such manifest").
    # Both images are released in lockstep — bump them together.
    vsphere_plugin_image: str = "everpure/client-plugin-vsphere:5.6.1"
    vsphere_plugin_proxy_image: str = "everpure/client-plugin-vsphere-reverse-proxy:5.6.1"

    # Toggle: never call out to real arrays/hypervisors (CI / demo)
    mock_mode: bool = False


@lru_cache
def get_settings() -> Settings:
    return Settings()
