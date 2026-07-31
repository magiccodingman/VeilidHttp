# Building the Electron Client

The Electron package always includes a platform-native `veilid-http-native` Rust
sidecar. Build on the target operating system/architecture unless you have deliberately
configured a complete cross-compilation toolchain.

## Linux

Install Rust 1.89+, Node.js 22+, pnpm 10+, and the system libraries required by
Electron/Veilid:

```bash
pnpm install --frozen-lockfile
cargo build --release -p veilid-http-native
pnpm --filter @veilid-http/electron test
pnpm --filter @veilid-http/electron build
pnpm --filter @veilid-http/electron make
```

Electron Forge currently has Debian and ZIP makers configured. AppImage support is a
future release-packaging task, not part of this development PR. Planned official Linux
releases target x64 first and arm64 after native/QEMU validation.

## Windows

Install Node.js 22+, pnpm 10+, Rust 1.89+, and the Visual Studio C++ build tools:

```powershell
pnpm install --frozen-lockfile
cargo build --release -p veilid-http-native
pnpm --filter @veilid-http/electron test
pnpm --filter @veilid-http/electron build
pnpm --filter @veilid-http/electron make
```

The Forge configuration packages `veilid-http-native.exe` and includes the Squirrel
maker. Official Windows binaries are deferred until code signing and release validation
are configured. Source builds remain supported.

## macOS

Install Xcode command-line tools, Node.js 22+, pnpm 10+, and Rust 1.89+:

```bash
pnpm install --frozen-lockfile
cargo build --release -p veilid-http-native
pnpm --filter @veilid-http/electron test
pnpm --filter @veilid-http/electron build
pnpm --filter @veilid-http/electron package
```

The repository supports local source builds. No official signed/notarized macOS binary
is planned. A developer distributing a macOS build is responsible for signing,
notarization, and testing the sidecar/socket behavior on that platform.

## Docker server

The server is independent of the desktop packaging and builds one multi-architecture
image:

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f docker/Dockerfile \
  -t veilid-http-server:dev \
  .
```

The current CI builds each architecture separately without publishing. Future release
workflows may create and push one manifest list only after both platform builds and
integration tests pass.
