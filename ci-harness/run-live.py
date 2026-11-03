#!/usr/bin/env python3
"""Run original Qwen CLI cases against local Ollama and retain real wire evidence."""
import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MODEL = 'qwen3.5:4b'
BACKEND = ('127.0.0.1', 11434)
REP = 'cli/file-system.test.ts > file-system > should be able to read a file'
HOP_HEADERS = {'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization',
               'te', 'trailer', 'transfer-encoding', 'upgrade', 'host'}


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def resources():
    result = {'time': time.time(), 'diskFreeBytes': shutil.disk_usage(Path.cwd()).free}
    info = Path('/proc/meminfo')
    if info.exists():
        values = {line.split(':')[0]: int(line.split()[1]) * 1024
                  for line in info.read_text().splitlines() if len(line.split()) >= 2}
        result['memoryAvailableBytes'] = values.get('MemAvailable', values.get('MemFree'))
        result['swapUsedBytes'] = values.get('SwapTotal', 0) - values.get('SwapFree', 0)
    return result


def local_json(path, payload=None):
    connection = http.client.HTTPConnection(*BACKEND, timeout=15)
    try:
        connection.request('GET' if payload is None else 'POST', path,
                           body=None if payload is None else json.dumps(payload),
                           headers={'Content-Type': 'application/json'})
        response = connection.getresponse()
        body = response.read()
        if response.status != 200:
            raise RuntimeError(f'Local Ollama {path}: HTTP {response.status}: {body[:500]!r}')
        return json.loads(body)
    finally:
        connection.close()


class Recorder:
    def __init__(self, directory):
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)
        self.records = []
        self.lock = threading.Lock()
        self.case = None
        self.active = 0
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_GET(self):
                self.forward()

            def do_POST(self):
                self.forward()

            def forward(self):
                if not self.path.startswith('/v1/'):
                    self.close_connection = True
                    return
                body = self.rfile.read(int(self.headers.get('Content-Length', '0')))
                with owner.lock:
                    index = len(owner.records) + 1
                    record = {'index': index, 'case': owner.case, 'method': self.command,
                              'path': self.path, 'startTime': time.time(),
                              'requestSha256': hashlib.sha256(body).hexdigest()}
                    owner.records.append(record)
                    owner.active += 1
                request_path = owner.directory / f'{index:04d}-request.json'
                request_path.write_bytes(body)
                try:
                    data = json.loads(body) if body else {}
                    record['model'] = data.get('model')
                    record['stream'] = data.get('stream')
                except ValueError:
                    record['requestJsonInvalid'] = True
                connection = http.client.HTTPConnection(*BACKEND, timeout=900)
                try:
                    # Preserve request bytes and non-hop headers. No model, prompt,
                    # tool, generation parameter or answer is rewritten here.
                    headers = {key: value for key, value in self.headers.items()
                               if key.lower() not in HOP_HEADERS}
                    connection.request(self.command, self.path, body=body or None, headers=headers)
                    record['forwardedRequestSha256'] = hashlib.sha256(body).hexdigest()
                    response = connection.getresponse()
                    record['httpStatus'] = response.status
                    self.send_response(response.status, response.reason)
                    for key, value in response.getheaders():
                        if key.lower() not in HOP_HEADERS:
                            self.send_header(key, value)
                    self.send_header('Connection', 'close')
                    self.end_headers()
                    digest = hashlib.sha256()
                    with (owner.directory / f'{index:04d}-response.wire').open('wb') as sink:
                        while True:
                            chunk = response.read1(65536)
                            if not chunk:
                                break
                            sink.write(chunk)
                            sink.flush()
                            digest.update(chunk)
                            self.wfile.write(chunk)
                            self.wfile.flush()
                    record['responseSha256'] = digest.hexdigest()
                    record['completed'] = True
                except Exception as error:
                    record['transportError'] = repr(error)
                    self.close_connection = True
                finally:
                    connection.close()
                    record['endTime'] = time.time()
                    with owner.lock:
                        owner.active -= 1
                    owner.save()

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f'http://127.0.0.1:{self.server.server_port}/v1'

    def save(self):
        with self.lock:
            write_json(self.directory / 'requests.json', list(self.records))

    def drain(self):
        deadline = time.time() + 8
        while self.active and time.time() < deadline:
            time.sleep(.1)

    def close(self):
        self.drain()
        self.server.shutdown()
        self.server.server_close()
        self.save()


def stream_parts(path):
    text = path.read_text(errors='replace')
    content = ''
    calls = {}
    if not text.lstrip().startswith('data:'):
        try:
            message = json.loads(text)['choices'][0].get('message', {})
            return message.get('content') or '', message.get('tool_calls', [])
        except (ValueError, KeyError, IndexError):
            return '', []
    for line in text.splitlines():
        if not line.startswith('data:') or line[5:].strip() == '[DONE]':
            continue
        try:
            frame = json.loads(line[5:].strip())
        except ValueError:
            continue
        for choice in frame.get('choices', []):
            delta = choice.get('delta', {})
            content += delta.get('content') or ''
            for item in delta.get('tool_calls', []):
                call = calls.setdefault(item.get('index', 0),
                                        {'id': '', 'name': '', 'arguments': ''})
                call['id'] += item.get('id') or ''
                call['name'] += item.get('function', {}).get('name') or ''
                call['arguments'] += item.get('function', {}).get('arguments') or ''
    return content, list(calls.values())


def verify_representative(recorder, records):
    reads = {}
    for record in records:
        response = recorder.directory / f"{record['index']:04d}-response.wire"
        if not response.exists():
            continue
        _, calls = stream_parts(response)
        for call in calls:
            function = call.get('function', call)
            if function.get('name') != 'read_file':
                continue
            arguments = function.get('arguments') or '{}'
            try:
                arguments = json.loads(arguments) if isinstance(arguments, str) else arguments
            except ValueError:
                continue
            if Path(arguments.get('file_path', '')).name == 'test.txt' and call.get('id'):
                reads[call['id']] = arguments
    for record in records:
        request = recorder.directory / f"{record['index']:04d}-request.json"
        try:
            data = json.loads(request.read_bytes())
        except ValueError:
            continue
        paired = [message for message in data.get('messages', [])
                  if message.get('role') == 'tool'
                  and message.get('tool_call_id') in reads
                  and 'hello world' in json.dumps(message.get('content'), ensure_ascii=False)]
        response = recorder.directory / f"{record['index']:04d}-response.wire"
        if paired and response.exists():
            content, _ = stream_parts(response)
            if 'hello world' in content:
                return {'passed': True, 'readCalls': reads,
                        'pairedResultRequest': record['index'], 'finalText': content}
    return {'passed': False, 'readCalls': reads,
            'reason': 'Missing actual read_file(test.txt), paired hello world result, or final hello world text'}


def stop_process(child):
    if child.poll() is None:
        os.killpg(child.pid, signal.SIGTERM)
        try:
            child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait(timeout=5)


def run_case(root, phase_dir, full_name, index, recorder):
    recorder.drain()
    recorder.case = full_name
    begin = len(recorder.records)
    directory = phase_dir / f'{index:02d}-{hashlib.sha256(full_name.encode()).hexdigest()[:8]}'
    directory.mkdir(parents=True, exist_ok=True)
    home = directory / 'isolated-home'
    config = home / 'config'
    config.mkdir(parents=True, exist_ok=True)
    (home / 'tmp').mkdir(exist_ok=True)
    system = home / 'system.json'
    system.write_text('{}')
    settings = {'security': {'auth': {'selectedType': 'openai'},
                             'folderTrust': {'enabled': False}},
                'model': {'name': MODEL, 'baseUrl': recorder.base_url,
                          'skipNextSpeakerCheck': True},
                'modelProviders': {'openai': [{'id': MODEL, 'name': 'Local Qwen',
                                               'baseUrl': recorder.base_url,
                                               'envKey': 'OPENAI_API_KEY',
                                               'generationConfig': {'contextWindowSize': 32768, 'timeout': 850000, 'maxRetries': 0,
                                                                    'samplingParams': {'temperature': 0, 'max_tokens': 2048},
                                                                    'extra_body': {'reasoning_effort': 'none'}}}]},
                'memory': {'enableManagedAutoMemory': False, 'enableManagedAutoDream': False},
                'general': {'enableAutoUpdate': False}}
    write_json(config / 'settings.json', settings)
    guard = phase_dir / 'loopback-guard.cjs'
    guard.write_text("""const fs=require('node:fs'),net=require('node:net');
function check(host){if(host&&host!=='localhost'&&host!=='::1'&&host!=='[::1]'&&!host.startsWith('127.')){fs.appendFileSync(process.env.QA_NETWORK_GUARD_LOG,JSON.stringify({pid:process.pid,deniedHost:host})+'\\n');throw new Error('CI forbids non-loopback connection');}}
const connect=net.Socket.prototype.connect;
net.Socket.prototype.connect=function(...args){const [options]=net._normalizeArgs(args);if(!options.path)check(options.host||'localhost');return connect.apply(this,args);};
const fetch=globalThis.fetch;if(fetch)globalThis.fetch=function(input,...rest){check(new URL(typeof input==='string'||input instanceof URL?input:input.url).hostname);return fetch.call(this,input,...rest);};
""")
    env = {'PATH': os.environ['PATH'], 'HOME': str(home), 'USERPROFILE': str(home),
           'TMPDIR': str(home / 'tmp'), 'CI': 'true', 'TERM': 'dumb', 'NO_COLOR': '1',
           'QWEN_HOME': str(config), 'QWEN_RUNTIME_DIR': str(home / 'runtime'),
           'QWEN_CODE_SYSTEM_SETTINGS_PATH': str(system),
           'QWEN_CODE_SYSTEM_DEFAULTS_PATH': str(system),
           'QWEN_CODE_MODELS_DEV_REFRESH': 'off', 'QWEN_SANDBOX': 'false',
           'QWEN_BASELINE_SKIP_PROMPT_LATENCY': '1',
           'OPENAI_API_KEY': 'synthetic-local-no-account', 'OPENAI_BASE_URL': recorder.base_url,
           'OPENAI_MODEL': MODEL, 'QWEN_MODEL': MODEL, 'NO_PROXY': 'localhost,127.0.0.1',
           'NODE_OPTIONS': '--require=' + str(guard),
           'QA_NETWORK_GUARD_LOG': str(directory / 'network-denials.jsonl')}
    if os.environ.get('TB_TIMEOUT_MINUTES'):
        env['TB_TIMEOUT_MINUTES'] = os.environ['TB_TIMEOUT_MINUTES']
    file, *titles = full_name.split(' > ')
    title = ' '.join(titles)
    command = [shutil.which('node'), str(root / 'node_modules/vitest/vitest.mjs'),
               'run', '--root', './integration-tests', '--config', './vitest.config.ts', file,
               '-t', '^' + re.escape(title) + '$', '--fileParallelism=false', '--maxWorkers=1',
               '--retry=0', '--reporter=default', '--reporter=json',
               '--outputFile=' + str(directory / 'vitest.json')]
    write_json(directory / 'command.json', {'command': command, 'env': env,
                                           'originalCase': full_name, 'cwd': str(root)})
    started = time.time()
    initial = resources()
    reason = None
    watchdog = max(900, float(env.get('TB_TIMEOUT_MINUTES', '5')) * 60 + 120)
    print(f'::group::{full_name}', flush=True)
    print('START', full_name, flush=True)
    with (directory / 'vitest.log').open('w') as log:
        child = subprocess.Popen(command, cwd=root, env=env, stdout=log,
                                 stderr=subprocess.STDOUT, start_new_session=True)
        while child.poll() is None:
            current = resources()
            with (directory / 'resources.jsonl').open('a') as sink:
                sink.write(json.dumps(current) + '\n')
            if current['diskFreeBytes'] < 16 * 1024 ** 3:
                reason = 'DISK_BELOW_16_GIB'
            if current.get('memoryAvailableBytes', 1024 ** 3) < 512 * 1024 ** 2:
                reason = 'MEMORY_AVAILABLE_BELOW_512_MIB'
            if current.get('swapUsedBytes', 0) - initial.get('swapUsedBytes', 0) > 256 * 1024 ** 2:
                reason = 'SWAP_GROWTH_OVER_256_MIB'
            if time.time() - started > watchdog:
                reason = 'PROCESS_WATCHDOG_AFTER_ORIGINAL_VITEST_TIMEOUT'
            if reason:
                stop_process(child)
                break
            time.sleep(1)
    recorder.drain()
    records = recorder.records[begin:]
    selected = []
    if (directory / 'vitest.json').exists():
        report = json.loads((directory / 'vitest.json').read_text())
        selected = [case for file_report in report.get('testResults', [])
                    for case in file_report.get('assertionResults', [])
                    if case.get('status') not in ('pending', 'todo', 'skipped')]
    exact = len(selected) == 1 and selected[0]['fullName'] == title
    passed = exact and selected[0]['status'] == 'passed' and child.returncode == 0
    inference = [record for record in records
                 if record['method'] == 'POST' and record['path'] in
                 ('/v1/chat/completions', '/v1/responses', '/v1/completions')]
    mismatches = [record['index'] for record in inference if record.get('model') != MODEL]
    result = {'case': full_name, 'status': 'PASS' if passed else 'FAIL' if exact else 'NOT_EXECUTED',
              'selectedCount': len(selected), 'selectedNames': [case['fullName'] for case in selected],
              'exitCode': child.returncode, 'seconds': time.time() - started,
              'inferenceRequests': len(inference), 'requestIds': [r['index'] for r in records],
              'modelMismatches': mismatches, 'resourceStop': reason,
              'failureMessages': selected[0].get('failureMessages', []) if exact else [],
              'evidence': str(directory)}
    if mismatches:
        result['status'] = 'CONFIG_MODEL_MISMATCH'
    if full_name == REP:
        result['liveVerification'] = verify_representative(recorder, records)
        if result['status'] == 'PASS' and not result['liveVerification']['passed']:
            result['status'] = 'LIVE_REP_VERIFICATION_FAILED'
    write_json(directory / 'result.json', result)
    print('RESULT', json.dumps(result), flush=True)
    if result['status'] != 'PASS':
        print((directory / 'vitest.log').read_text(errors='replace')[-12000:], flush=True)
    print('::endgroup::', flush=True)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('phase', choices=('rep', 'rest'))
    args = parser.parse_args()
    root = Path.cwd().resolve()
    expected = json.loads(Path(__file__).with_name('expected-cases.json').read_text())
    assert len(expected) == len(set(expected)) == 43 and REP in expected
    evidence = Path(os.environ['RUNNER_TEMP']) / 'live-evidence'
    evidence.mkdir(parents=True, exist_ok=True)
    phase_dir = evidence / args.phase
    phase_dir.mkdir(parents=True, exist_ok=True)
    sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
    requested_sha = os.environ.get('EXPECTED_CANDIDATE_SHA') or os.environ.get('CANDIDATE_SHA')
    if requested_sha and sha != requested_sha:
        raise RuntimeError(f'Candidate SHA mismatch: {sha} != {requested_sha}')
    if not (root / 'dist/cli.js').is_file() or not (root / 'node_modules/vitest/vitest.mjs').is_file():
        raise RuntimeError('Build/bundle or workspace dependencies are missing')
    if resources()['diskFreeBytes'] < 16 * 1024 ** 3:
        raise RuntimeError('Disk below the 16GiB floor before tests')
    metadata = {'sha': sha, 'phase': args.phase, 'model': MODEL, 'backend': 'http://127.0.0.1:11434',
                'node': subprocess.check_output(['node', '--version'], text=True).strip(),
                'version': local_json('/api/version'), 'models': local_json('/v1/models'),
                'details': local_json('/api/show', {'model': MODEL}), 'startedAt': time.time()}
    write_json(phase_dir / 'metadata.json', metadata)
    if MODEL not in [entry['id'] for entry in metadata['models']['data']]:
        raise RuntimeError('The expected local model is unavailable')
    rows = []
    if args.phase == 'rest':
        rep = json.loads((evidence / 'rep/summary.json').read_text())
        if rep['sha'] != sha or rep['passed'] != 1 or not rep['cases'][0]['liveVerification']['passed']:
            raise RuntimeError('The same-SHA real representative has not passed')
        rows = list(rep['cases'])
    cases = [REP] if args.phase == 'rep' else [case for case in expected if case != REP]
    recorder = Recorder(phase_dir / 'http')
    fatal = None
    try:
        for index, case in enumerate(cases, 1):
            result = run_case(root, phase_dir, case, index, recorder)
            rows.append(result)
            by_name = {row['case']: row for row in rows}
            coverage = [by_name.get(name, {'case': name, 'status': 'NOT_STARTED',
                                         'selectedCount': 0, 'inferenceRequests': 0}) for name in expected]
            write_json(evidence / 'coverage.json', coverage)
            summary = {'sha': sha, 'phase': args.phase, 'passed': sum(r['status'] == 'PASS' for r in rows),
                       'attempted': len(rows), 'originalAssertionsExecuted': sum(r['selectedCount'] == 1 for r in rows),
                       'cases': rows, 'updatedAt': time.time()}
            write_json(phase_dir / 'summary.json', summary)
            print('PROGRESS', args.phase, index, '/', len(cases), 'PASS', summary['passed'], flush=True)
            if result['resourceStop']:
                fatal = result['resourceStop']
                break
            # Independent failed assertions do not prevent the other original cases.
    finally:
        recorder.close()
    summary = {'sha': sha, 'phase': args.phase, 'attempted': len(rows),
               'originalAssertionsExecuted': sum(r['selectedCount'] == 1 for r in rows),
               'passed': sum(r['status'] == 'PASS' for r in rows),
               'failedOrBlocked': sum(r['status'] != 'PASS' for r in rows),
               'cases': rows, 'fatal': fatal, 'finishedAt': time.time()}
    write_json(phase_dir / 'summary.json', summary)
    print('PHASE_COMPLETE', json.dumps({key: value for key, value in summary.items() if key != 'cases'}), flush=True)
    return 0 if not fatal and all(row['status'] == 'PASS' for row in rows) else 1


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception:
        import traceback
        traceback.print_exc()
        raise SystemExit(2)
