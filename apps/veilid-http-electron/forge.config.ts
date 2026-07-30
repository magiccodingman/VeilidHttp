import type { ForgeConfig } from '@electron-forge/shared-types';
import { MakerDeb } from '@electron-forge/maker-deb';
import { MakerSquirrel } from '@electron-forge/maker-squirrel';
import { MakerZIP } from '@electron-forge/maker-zip';
import { WebpackPlugin } from '@electron-forge/plugin-webpack';
import { mainConfig } from './webpack.main.config';
import { rendererConfig } from './webpack.renderer.config';

const config: ForgeConfig = {
  packagerConfig: {
    asar: true,
    executableName: 'veilid-http',
    extraResource: ['../../target/release/veilid-http-native'],
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
