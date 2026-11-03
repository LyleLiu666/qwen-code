import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve, join } from 'node:path';
import { pathToFileURL } from 'node:url';

const [repo, argPath] = process.argv.slice(2);
const load = (path) => import(pathToFileURL(resolve(repo, 'packages/core/dist/src', path)).href);
const [{ NotebookEditTool }, { makeFakeConfig }] = await Promise.all([load('tools/notebook-edit.js'), load('test-utils/config.js')]);
const fixture = await mkdtemp(join(tmpdir(), 'q15595-notebook-control-'));
const file = join(fixture, 'workflow.ipynb');
const source = '## Inserted Note\nThis cell was inserted by NotebookEdit.';
const initial = { cells: [
  {id:'intro',cell_type:'markdown',metadata:{},source:['# Workflow\n']},
  {id:'remove-me',cell_type:'markdown',metadata:{},source:['This temporary cell should be deleted.\n']},
  {id:'calculate',cell_type:'code',metadata:{},source:['value = 10\n'],execution_count:null,outputs:[]}
], metadata:{kernelspec:{display_name:'Python 3',language:'python',name:'python3'},language_info:{name:'python'}}, nbformat:4, nbformat_minor:5 };
try {
  const config = makeFakeConfig({targetDir:fixture, cwd:fixture, telemetry:{enabled:false}, deferTelemetryInitialization:true});
  const tool=new NotebookEditTool(config);
  const steps=argPath?JSON.parse(await readFile(argPath,'utf8')):[
    {cell_id:'intro',edit_mode:'insert',new_source:source,cell_type:'markdown'},
    {cell_id:'remove-me',edit_mode:'delete'}];
  await writeFile(file,JSON.stringify(initial,null,1));
  const outcomes=[];
  for (const args of steps) {
    await readFile(file);
    config.getFileReadCache().recordRead(file,await stat(file),{full:true,cacheable:true});
    const result=await tool.build({...args,notebook_path:file}).execute(new AbortController().signal);
    outcomes.push({args,error:result.error??null});
  }
  const cells=JSON.parse(await readFile(file,'utf8')).cells;
  const inserted=cells.find(c=>[].concat(c.source).join('').includes('Inserted Note'));
  if (!argPath) assert.equal(inserted?.cell_type,'markdown');
  console.log(JSON.stringify({outcomes,cells,insertedType:inserted?.cell_type}));
} finally { await rm(fixture,{recursive:true,force:true}); }
