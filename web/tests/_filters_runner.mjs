// Runs shared/filters.js under Node and emits its output as JSON, so
// test_filters_parity.py can diff it against api/columns.py.
//
// filters.js is loaded via a data: URL rather than a plain import because it
// is an ES module living in a directory with no package.json — Node would
// otherwise parse it as CommonJS and choke on `export`. Doing it this way
// keeps the repo free of a package.json that Vercel might interpret as a
// build signal while the site is still deployed there.
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const src = readFileSync(join(here, '..', 'shared', 'filters.js'), 'utf8');
const mod = await import(
  'data:text/javascript;base64,' + Buffer.from(src).toString('base64')
);

const corpus = JSON.parse(readFileSync(0, 'utf8'));
const out = corpus.map((params) => {
  try {
    const { clauses, bindValues } = mod.parseFilters(params);
    return { clauses, binds: bindValues, error: null };
  } catch (e) {
    return { clauses: null, binds: null, error: String(e.message || e) };
  }
});
process.stdout.write(JSON.stringify(out));
