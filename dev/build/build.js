import { build } from 'esbuild';

build({
  entryPoints: ['../functions/convert/src/index.ts'],
  bundle: true,
  platform: 'node',
  outfile: '../../dist/convert_sqs/index.ts',
  target: 'node18',
}).catch(() => process.exit(1));

build({
  entryPoints: ['../functions/process/src/index.ts'],
  bundle: true,
  platform: 'node',
  outfile: '../../dist/process_sm/index.ts',
  target: 'node20',
}).catch(() => process.exit(1));

build({
  entryPoints: ['../functions/merger/src/index.ts'],
  bundle: true,
  platform: 'node',
  outfile: '../../dist/merger_sm/index.ts',
  target: 'node20',
}).catch(() => process.exit(1));
