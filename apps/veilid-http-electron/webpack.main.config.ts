import type { Configuration } from 'webpack';
export const mainConfig: Configuration = {
  entry: './src/main/index.ts',
  module: { rules: [{ test: /\.tsx?$/, exclude: /node_modules/, use: 'ts-loader' }] },
  resolve: { extensions: ['.ts', '.js'] },
};
