import type { ForgeConfig } from '@electron-forge/shared-types';
import { MakerDeb } from '@electron-forge/maker-deb';
import { MakerSquirrel } from '@electron-forge/maker-squirrel';
import { MakerZIP } from '@electron-forge/maker-zip';
import { WebpackPlugin } from '@electron-forge/plugin-webpack';
import { flipFuses, FuseV1Options, FuseVersion } from '@electron/fuses';
import path from 'node:path';
import { mainConfig } from './webpack.main.config';
import { rendererConfig } from './webpack.renderer.config';

const sidecarName = process.platform === 'win32' ? 'veilid-http-native.exe' : 'veilid-http-native';
const sidecarPath = path.resolve(__dirname, '../../target/release', sidecarName);

function electronBinaryPath(buildPath: string, platform: string): string {
  if (platform === 'darwin') {
    return path.join(buildPath, 'Electron.app', 'Contents', 'MacOS', 'Electron');
  }
  if (platform === 'win32') return path.join(buildPath, 'electron.exe');
  return path.join(buildPath, 'electron');
}

const config: ForgeConfig = {
  packagerConfig: {
    asar: true,
    executableName: 'veilid-http',
    extraResource: [sidecarPath],
  },
  hooks: {
    packageAfterExtract: async (_forgeConfig, buildPath, _electronVersion, platform) => {
      await flipFuses(electronBinaryPath(buildPath, platform), {
        version: FuseVersion.V1,
        [FuseV1Options.RunAsNode]: false,
        [FuseV1Options.EnableCookieEncryption]: true,
        [FuseV1Options.EnableNodeOptionsEnvironmentVariable]: false,
        [FuseV1Options.EnableNodeCliInspectArguments]: false,
        [FuseV1Options.EnableEmbeddedAsarIntegrityValidation]: true,
        [FuseV1Options.OnlyLoadAppFromAsar]: true,
      });
    },
  },
  makers: [
    new MakerDeb({ options: { maintainer: 'VeilidHttp contributors', homepage: 'https://github.com/magiccodingman/VeilidHttp' } }),
    new MakerSquirrel({}),
    new MakerZIP({}, ['darwin']),
  ],
  plugins: [
    new WebpackPlugin({
      mainConfig,
      renderer: {
        config: rendererConfig,
        entryPoints: [{ html: './src/shell/index.html', js: './src/shell/index.ts', name: 'shell', preload: { js: './src/preload/shell.ts' } }],
      },
    }),
  ],
};

export default config;
