# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2025-06-29

### Added

- Added AsyncRead/AsyncWrite support for Tag and Stream (requires `utils` feature flag)
- Added `connect_addr_vec` function to Worker

### Changed

- Updated to UCX 1.18 with latest API compatibility
- Updated multiple dependency versions
- Migrated to Rust 2021 edition

### Fixed

- Fixed various bugs and issues

## [0.1.1] - 2022-09-01

### Changed

- Replace dependency `os_socketaddr` by `socket2`. #5

## [0.1.0] - 2022-04-20

### Added

- Asynchronous UCP bindings.
