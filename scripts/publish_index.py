#!/usr/bin/env python3
"""Write the PEP 503 wheel index served on GitHub Pages from the release assets.

Run by `.github/workflows/wheels.yml` after each upload. Lists every ``.whl``
asset on the repo's GitHub releases, optionally deletes dev builds beyond the
newest ``--keep`` per project from the rolling ``main-dev`` release, and writes
``<out>/simple/index.html`` plus one page per project. Links point at the
release download URLs with a ``#sha256=`` fragment (from the asset's
``digest``; downloaded and hashed when GitHub has none).

    GH_TOKEN=... python scripts/publish_index.py --repo owner/name --out site \
        --keep 30 --prune

Stdlib only: it runs on a bare runner Python.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import sys
import urllib.request
from dataclasses import dataclass
from pathlib import Path

API = "https://api.github.com"
DEV_RELEASE = "main-dev"
REQUIRES_PYTHON = {
    "rithmic-gateway": ">=3.12",
    "rithmic-nt-connect": ">=3.12,<3.15",
}
_WHEEL = re.compile(r"^(?P<name>[A-Za-z0-9_.]+?)-(?P<version>[^-]+)-.+\.whl$")


@dataclass(frozen=True)
class Wheel:
    filename: str
    project: str
    version: str
    url: str
    sha256: str | None
    asset_id: int
    release: str


def normalize(name: str) -> str:
    """PEP 503 project name normalization."""
    return re.sub(r"[-_.]+", "-", name).lower()


def version_key(version: str) -> tuple[tuple[int, ...], float]:
    """Sort key for the versions this workflow produces (``X.Y.Z[.devN]``)."""
    base, _, dev = version.partition(".dev")
    nums = tuple(int(p) for p in base.split(".") if p.isdigit())
    return nums, (int(dev) if dev.isdigit() else float("inf"))


def wheels_from_releases(releases: list[dict]) -> list[Wheel]:
    out: list[Wheel] = []
    for release in releases:
        if release.get("draft"):
            continue
        for asset in release.get("assets", []):
            m = _WHEEL.match(asset["name"])
            if m is None:
                continue
            digest = asset.get("digest") or ""
            out.append(
                Wheel(
                    filename=asset["name"],
                    project=normalize(m["name"]),
                    version=m["version"],
                    url=asset["browser_download_url"],
                    sha256=digest.removeprefix("sha256:") or None,
                    asset_id=asset["id"],
                    release=release["tag_name"],
                )
            )
    return out


def split_pruned(wheels: list[Wheel], keep: int) -> tuple[list[Wheel], list[Wheel]]:
    """Keep every tagged-release wheel and the newest ``keep`` dev versions per
    project; return ``(kept, pruned)``."""
    dev_versions: dict[str, set[str]] = {}
    for w in wheels:
        if w.release == DEV_RELEASE:
            dev_versions.setdefault(w.project, set()).add(w.version)
    keep_versions = {
        project: set(sorted(versions, key=version_key, reverse=True)[:keep])
        for project, versions in dev_versions.items()
    }
    kept: list[Wheel] = []
    pruned: list[Wheel] = []
    for w in wheels:
        if w.release == DEV_RELEASE and w.version not in keep_versions[w.project]:
            pruned.append(w)
        else:
            kept.append(w)
    return kept, pruned


def render(wheels: list[Wheel]) -> dict[str, str]:
    """Relative path -> HTML for the root and per-project simple pages."""
    by_project: dict[str, list[Wheel]] = {}
    for w in wheels:
        by_project.setdefault(w.project, []).append(w)

    def page(title: str, body: list[str]) -> str:
        return (
            '<!DOCTYPE html>\n<html><head><meta name="pypi:repository-version" '
            f'content="1.0"><title>{html.escape(title)}</title></head><body>\n'
            + "\n".join(body)
            + "\n</body></html>\n"
        )

    pages = {
        "simple/index.html": page(
            "Simple index",
            [f'<a href="{p}/">{p}</a><br>' for p in sorted(by_project)],
        )
    }
    for project, items in by_project.items():
        items = sorted(
            items, key=lambda w: (version_key(w.version), w.filename), reverse=True
        )
        rp = REQUIRES_PYTHON.get(project)
        rp_attr = f' data-requires-python="{html.escape(rp)}"' if rp else ""
        links = []
        for w in items:
            href = w.url + (f"#sha256={w.sha256}" if w.sha256 else "")
            links.append(
                f'<a href="{html.escape(href)}"{rp_attr}>'
                f"{html.escape(w.filename)}</a><br>"
            )
        pages[f"simple/{project}/index.html"] = page(f"Links for {project}", links)
    return pages


# --- GitHub I/O -------------------------------------------------------------


def _request(method: str, url: str, token: str) -> urllib.request.Request:
    if not url.startswith("https://"):
        raise ValueError(f"refusing non-https URL: {url}")
    return urllib.request.Request(
        url,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )


def list_releases(repo: str, token: str) -> list[dict]:
    releases: list[dict] = []
    page = 1
    while True:
        url = f"{API}/repos/{repo}/releases?per_page=100&page={page}"
        with urllib.request.urlopen(_request("GET", url, token)) as r:  # nosec B310
            batch = json.load(r)
        releases.extend(batch)
        if len(batch) < 100:
            return releases
        page += 1


def delete_asset(repo: str, token: str, asset_id: int) -> None:
    url = f"{API}/repos/{repo}/releases/assets/{asset_id}"
    with urllib.request.urlopen(_request("DELETE", url, token)):  # nosec B310
        pass


def hash_download(url: str, token: str) -> str:
    digest = hashlib.sha256()
    req = _request("GET", url, token)
    req.add_header("Accept", "application/octet-stream")
    with urllib.request.urlopen(req) as r:  # nosec B310
        for chunk in iter(lambda: r.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--repo", required=True, help="owner/name")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--keep", type=int, default=30)
    ap.add_argument(
        "--prune", action="store_true", help="delete dev assets beyond --keep"
    )
    args = ap.parse_args(argv)
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        print("publish_index: GH_TOKEN is required", file=sys.stderr)
        return 2

    wheels = wheels_from_releases(list_releases(args.repo, token))
    kept, pruned = split_pruned(wheels, args.keep)
    if args.prune:
        for w in pruned:
            print(f"prune {w.release}/{w.filename}")
            delete_asset(args.repo, token, w.asset_id)
    else:
        kept = wheels

    resolved = [
        w
        if w.sha256
        else Wheel(**{**w.__dict__, "sha256": hash_download(w.url, token)})
        for w in kept
    ]
    for rel, text in render(resolved).items():
        path = args.out / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    print(f"index: {len(resolved)} wheels, {len(pruned)} pruned -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
