"""The PEP 503 index writer: pruning keeps tagged releases and the newest dev
builds; pages carry sha256 fragments and normalized project names."""

from __future__ import annotations

import publish_index as pi


def _asset(name: str, i: int, digest: str | None = "sha256:ab") -> dict:
    a = {"name": name, "id": i, "browser_download_url": f"https://x/{name}"}
    if digest is not None:
        a["digest"] = digest
    return a


def _releases(dev_versions: list[str]) -> list[dict]:
    dev = [
        _asset(f"rithmic_gateway-{v}-py3-none-macosx_11_0_arm64.whl", n)
        for n, v in enumerate(dev_versions)
    ]
    dev.append(_asset("notes.txt", 999))
    tagged = [
        _asset("rithmic_gateway-0.0.9-py3-none-macosx_11_0_arm64.whl", 1000),
        _asset(
            "rithmic_nt_connect-0.0.9-cp313-cp313-manylinux_2_35_x86_64.whl",
            1001,
            None,
        ),
    ]
    return [
        {"tag_name": pi.DEV_RELEASE, "assets": dev},
        {"tag_name": "v0.0.9", "assets": tagged},
        {"tag_name": "draft", "draft": True, "assets": [_asset("a-1-x.whl", 5)]},
    ]


def test_prune_keeps_newest_dev_versions_and_every_tagged_wheel() -> None:
    versions = [f"0.1.0.dev2026101{d}{n:05d}" for d, n in ((3, 9), (4, 10), (4, 11))]
    wheels = pi.wheels_from_releases(_releases(versions))
    assert {w.release for w in wheels} == {pi.DEV_RELEASE, "v0.0.9"}  # no draft

    kept, pruned = pi.split_pruned(wheels, keep=2)
    assert [w.version for w in pruned] == [versions[0]]
    assert {w.version for w in kept} == {versions[1], versions[2], "0.0.9"}


def test_version_key_orders_dev_builds_before_their_release() -> None:
    ordered = ["0.1.0.dev202610130009", "0.1.0.dev202610140010", "0.1.0", "0.2.0"]
    assert sorted(reversed(ordered), key=pi.version_key) == ordered


def test_pages_use_normalized_names_and_sha256_fragments() -> None:
    wheels = pi.wheels_from_releases(_releases(["0.1.0.dev202610140001"]))
    pages = pi.render(wheels)

    assert set(pages) == {
        "simple/index.html",
        "simple/rithmic-gateway/index.html",
        "simple/rithmic-nt-connect/index.html",
    }
    assert 'href="rithmic-gateway/"' in pages["simple/index.html"]
    gw = pages["simple/rithmic-gateway/index.html"]
    assert "#sha256=ab" in gw
    assert 'data-requires-python="&gt;=3.12"' in gw
    # Newest first.
    assert gw.index("dev202610140001") < gw.index("0.0.9-")
    # No digest: no fragment (main() hashes the download before rendering).
    assert "#sha256" not in pages["simple/rithmic-nt-connect/index.html"]
