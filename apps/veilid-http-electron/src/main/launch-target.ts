import fs from 'node:fs';
import path from 'node:path';

export interface LaunchTarget {
  routeBlobBase64: string;
  startPath: string;
}

interface Descriptor {
  schema?: unknown;
  routeBlob?: unknown;
  startPath?: unknown;
}

function parseDescriptor(filename: string): LaunchTarget {
  const parsed = JSON.parse(fs.readFileSync(filename, 'utf8')) as Descriptor;
  if (parsed.schema !== 'org.veilidhttp.app/v1') throw new Error('Unsupported .veilidapp schema');
  if (typeof parsed.routeBlob !== 'string' || parsed.routeBlob.length === 0) throw new Error('Descriptor RouteBlob is missing');
  if (parsed.startPath !== undefined && typeof parsed.startPath !== 'string') throw new Error('Descriptor startPath must be a string');
  return { routeBlobBase64: parsed.routeBlob, startPath: parsed.startPath ?? '/' };
}

export function findLaunchTarget(argv: string[], executableDirectory: string): LaunchTarget | undefined {
  let startPath = '/';
  const pathIndex = argv.indexOf('--path');
  if (pathIndex >= 0) {
    const value = argv[pathIndex + 1];
    if (!value) throw new Error('--path requires a value');
    startPath = value;
  }

  const base64Index = argv.indexOf('--route-base64');
  if (base64Index >= 0) {
    const routeBlobBase64 = argv[base64Index + 1];
    if (!routeBlobBase64) throw new Error('--route-base64 requires a value');
    return { routeBlobBase64, startPath };
  }

  const fileIndex = argv.indexOf('--route-file');
  if (fileIndex >= 0) {
    const filename = argv[fileIndex + 1];
    if (!filename) throw new Error('--route-file requires a value');
    return { routeBlobBase64: fs.readFileSync(filename).toString('base64url'), startPath };
  }

  const descriptorArgument = argv.find((value) => value.toLowerCase().endsWith('.veilidapp'));
  if (descriptorArgument) return parseDescriptor(descriptorArgument);

  const sibling = path.join(executableDirectory, 'app.veilidapp');
  return fs.existsSync(sibling) ? parseDescriptor(sibling) : undefined;
}
