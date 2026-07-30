# Building the Electron Client

## Linux

```bash
pnpm install
cargo build --release -p veilid-http-native
pnpm --filter @veilid-http/electron package
```

Future official releases will target AppImage and Debian packages on x64 and arm64.
Linux is the primary release platform.

## Windows

Install Node.js 22, pnpm 10, Rust 1.89, and the Visual Studio C++ build tools, then:

```powershell
pnpm install
cargo build --release -p veilid-http-native
pnpm --filter @veilid-http/electron package
```

Windows releases are planned after code signing is configured.

## macOS

Install Xcode command-line tools, Node.js 22, pnpm 10, and Rust 1.89:

```bash
pnpm install
cargo build --release -p veilid-http-native
pnpm --filter @veilid-http/electron package
```

The repository supports local source builds, but no official signed/notarized macOS
binary is planned.
