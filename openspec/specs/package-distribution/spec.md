# package-distribution Specification

## Purpose
How the `rithmic-gateway` and `rithmic-nt-connect` wheels are built in CI, smoke-tested without Rust, and published to the PEP 503 index consumers install from.

## Requirements

### Requirement: Prebuilt platform wheels on every push to main
CI SHALL build `rithmic-gateway` and `rithmic-nt-connect` wheels on every push to `main` and on every tag, for Linux x86_64, Linux ARM64 and macOS ARM64. Dev builds SHALL be versioned `X.Y.Z.devYYYYMMDDNNNNN` (date, then the zero-padded run number; no `+local` segment because GitHub renames release assets with special characters). Every wheel SHALL carry a build provenance attestation.

#### Scenario: Merge to main
- **WHEN** a commit lands on `main`
- **THEN** wheels for all three platforms SHALL be published under a new dev version

### Requirement: Wheels are installable from a package index
Wheels SHALL be published to a rolling `main` GitHub prerelease (and to a fixed release for tags) and listed in a PEP 503 index on GitHub Pages with sha256 hashes. The index SHALL keep at least the 30 most recent dev builds. Nothing SHALL be published to PyPI.

#### Scenario: Consumer install
- **WHEN** a project configures the index and runs `uv add --prerelease allow rithmic-gateway`
- **THEN** the latest dev wheel for its platform SHALL install with no Rust, protoc or maturin on that machine

### Requirement: The gateway wheel carries the parent binary
The `rithmic-gateway` wheel SHALL install the `rithmic-gateway` binary into the environment's scripts directory, alongside the `rithmic_gateway` Python package, and depend only on `protobuf`.

#### Scenario: Binary available after install
- **WHEN** the wheel is installed into a clean virtual environment
- **THEN** `rithmic-gateway --version` and `python -m rithmic_gateway --help` SHALL succeed

### Requirement: Install-without-Rust smoke in CI
The wheel workflow SHALL install each built wheel into a clean environment with no Rust toolchain, import `GatewayClient`, run the binary, and run the client scenario subset against the fake parent before publishing.

#### Scenario: Broken wheel
- **WHEN** a built wheel lacks the binary or fails to import
- **THEN** the workflow SHALL fail and nothing SHALL be published
