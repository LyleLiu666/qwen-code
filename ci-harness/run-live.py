#!/usr/bin/env python3
"""Run original Qwen CLI cases against local Ollama and retain real wire evidence."""
import argparse
import fcntl
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import select
import shutil
import socket
import signal
import subprocess
import tempfile
import threading
import time
import uuid
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


class Connection:
    def __init__(self, downstream, record):
        self.downstream = downstream
        self.record = record
        self.upstream = None
        self.finished = threading.Event()
        self.aborted = threading.Event()
        self.lock = threading.Lock()

    def abort(self, reason, close_downstream=False):
        with self.lock:
            if not self.aborted.is_set():
                self.record['cancelReason'] = reason
                self.aborted.set()
            sockets = [self.upstream]
            if close_downstream:
                sockets.append(self.downstream)
            for connection in sockets:
                if connection is not None:
                    try:
                        connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass

    def watch_downstream(self):
        while not self.finished.wait(.05):
            try:
                readable, _, _ = select.select([self.downstream], [], [], .05)
                if readable and not self.downstream.recv(1, socket.MSG_PEEK):
                    self.abort('downstream_disconnected')
                    return
            except (OSError, ValueError):
                if not self.finished.is_set():
                    self.abort('downstream_disconnected')
                return


class Recorder:
    used_ports = set()

    def __init__(self, directory, case=None, port_registry=None):
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)
        self.records = []
        self.lock = threading.Lock()
        self.case = case
        self.connections = set()
        self.closed = threading.Event()
        if port_registry and port_registry.exists():
            self.used_ports.update(json.loads(port_registry.read_text()))
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_GET(self):
                self.forward()

            def do_POST(self):
                self.forward()

            def forward(self):
                if owner.closed.is_set() or not self.path.startswith('/v1/'):
                    self.close_connection = True
                    return
                expected_length = int(self.headers.get('Content-Length', '0'))
                body = self.rfile.read(expected_length)
                if len(body) != expected_length:
                    self.close_connection = True
                    return
                with owner.lock:
                    if owner.closed.is_set():
                        self.close_connection = True
                        return
                    index = len(owner.records) + 1
                    record = {'index': index, 'case': owner.case, 'method': self.command,
                              'path': self.path, 'startTime': time.time(),
                              'requestSha256': hashlib.sha256(body).hexdigest()}
                    owner.records.append(record)
                    control = Connection(self.connection, record)
                    owner.connections.add(control)
                (owner.directory / f'{index:04d}-request.json').write_bytes(body)
                try:
                    data = json.loads(body) if body else {}
                    record['model'] = data.get('model')
                    record['stream'] = data.get('stream')
                except ValueError:
                    record['requestJsonInvalid'] = True
                upstream = http.client.HTTPConnection(*BACKEND, timeout=5)
                watcher = None
                try:
                    upstream.connect()
                    upstream.sock.settimeout(900)
                    with control.lock:
                        control.upstream = upstream.sock
                    if owner.closed.is_set() or control.aborted.is_set():
                        control.abort('case_closed', close_downstream=True)
                        return
                    watcher = threading.Thread(target=control.watch_downstream, daemon=True)
                    watcher.start()
                    headers = {key: value for key, value in self.headers.items()
                               if key.lower() not in HOP_HEADERS}
                    upstream.request(self.command, self.path, body=body or None, headers=headers)
                    record['forwardedRequestSha256'] = hashlib.sha256(body).hexdigest()
                    response = upstream.getresponse()
                    record['httpStatus'] = response.status
                    self.send_response(response.status, response.reason)
                    for key, value in response.getheaders():
                        if key.lower() not in HOP_HEADERS:
                            self.send_header(key, value)
                    self.send_header('Connection', 'close')
                    self.end_headers()
                    digest = hashlib.sha256()
                    with (owner.directory / f'{index:04d}-response.wire').open('wb') as sink:
                        while not control.aborted.is_set():
                            chunk = response.read1(65536)
                            if not chunk:
                                break
                            sink.write(chunk)
                            sink.flush()
                            digest.update(chunk)
                            self.wfile.write(chunk)
                            self.wfile.flush()
                    record['responseSha256'] = digest.hexdigest()
                    record['completed'] = not control.aborted.is_set()
                except Exception as error:
                    record['transportError'] = repr(error)
                    control.abort('transport_error')
                    self.close_connection = True
                finally:
                    control.finished.set()
                    upstream.close()
                    if watcher:
                        watcher.join(timeout=1)
                    record['endTime'] = time.time()
                    with owner.lock:
                        owner.connections.discard(control)
                    owner.save()

        for _ in range(100):
            self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
            port = self.server.server_port
            if port not in self.used_ports:
                self.used_ports.add(port)
                break
            self.server.server_close()
        else:
            raise RuntimeError('Could not allocate an unused case recorder port')
        if port_registry:
            write_json(port_registry, sorted(self.used_ports))
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f'http://127.0.0.1:{port}/v1'

    def save(self):
        with self.lock:
            write_json(self.directory / 'requests.json', list(self.records))

    def close(self):
        self.closed.set()
        self.server.shutdown()
        self.server.server_close()
        deadline = time.monotonic() + 8
        while True:
            with self.lock:
                active = list(self.connections)
            if not active:
                break
            for control in active:
                control.abort('case_cleanup', close_downstream=True)
            if time.monotonic() >= deadline:
                raise RuntimeError('Case recorder still has in-flight requests after cancellation')
            time.sleep(.05)
        self.thread.join(timeout=1)
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


def process_table(proc_root=Path('/proc')):
    table = {}
    if proc_root.exists():
        for entry in proc_root.iterdir():
            if not entry.name.isdigit():
                continue
            try:
                tail = (entry / 'stat').read_text().rsplit(') ', 1)[1].split()
                table[int(entry.name)] = {'ppid': int(tail[1]), 'pgid': int(tail[2]),
                                          'identity': tail[19], 'state': tail[0]}
            except (OSError, ValueError, IndexError):
                pass
    else:
        output = subprocess.check_output(['ps', '-axo', 'pid=,ppid=,pgid=,stat='], text=True)
        for line in output.splitlines():
            pid, ppid, pgid, state = line.split(None, 3)
            table[int(pid)] = {'ppid': int(ppid), 'pgid': int(pgid),
                               'identity': None, 'state': state}
    return table


def marked_processes(key, value, proc_root=Path('/proc')):
    found = set()
    if not proc_root.exists():
        return found
    stamp = (key + '=' + value).encode()
    for entry in proc_root.iterdir():
        if entry.name.isdigit():
            try:
                if stamp in (entry / 'environ').read_bytes().split(b'\0'):
                    found.add(int(entry.name))
            except OSError:
                pass
    return found


class CaseProcesses:
    def __init__(self, child=None, marker=None, home=None, proc_root=Path('/proc')):
        self.child = child
        self.marker = marker
        self.home = home
        self.proc_root = proc_root
        self.group = child.pid if child else None
        self.owned = {}
        self.observed = set()
        self.refresh()

    def refresh(self):
        table = process_table(self.proc_root)
        candidates = {pid for pid, identity in self.owned.items()
                      if pid in table and table[pid]['identity'] == identity}
        if self.child:
            candidates.add(self.child.pid)
        if self.marker:
            candidates.update(marked_processes('QWEN_LIVE_CASE_TOKEN', self.marker, self.proc_root))
        if self.home:
            candidates.update(marked_processes('QWEN_LIVE_CASE_HOME', self.home, self.proc_root))
        changed = True
        while changed:
            descendants = {pid for pid, info in table.items()
                           if info['ppid'] in candidates or (self.group and info['pgid'] == self.group)}
            changed = not descendants.issubset(candidates)
            candidates.update(descendants)
        live = {}
        for pid in candidates:
            info = table.get(pid)
            if not info or pid == os.getpid() or info['state'].startswith('Z'):
                continue
            identity = info['identity']
            if pid in self.owned and self.owned[pid] != identity:
                continue
            self.owned[pid] = identity
            self.observed.add(pid)
            live[pid] = identity
        return live

    def signal(self, sig):
        live = self.refresh()
        # The leader can already have exited while its process group remains.
        if self.group and self.group != os.getpgrp():
            try:
                os.killpg(self.group, sig)
            except ProcessLookupError:
                pass
        for pid, identity in live.items():
            current = process_table(self.proc_root).get(pid)
            if current and current['identity'] == identity:
                try:
                    os.kill(pid, sig)
                except ProcessLookupError:
                    pass

    def cleanup(self):
        self.signal(signal.SIGTERM)
        deadline = time.monotonic() + 2
        while self.refresh() and time.monotonic() < deadline:
            time.sleep(.1)
        if self.refresh():
            self.signal(signal.SIGKILL)
        if self.child:
            try:
                self.child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.signal(signal.SIGKILL)
                self.child.wait(timeout=5)
        deadline = time.monotonic() + 3
        while self.refresh() and time.monotonic() < deadline:
            time.sleep(.1)
        return {'observedPids': sorted(self.observed), 'remainingPids': sorted(self.refresh())}


def run_case(root, phase_dir, full_name, index, evidence, target=False):
    directory = phase_dir / f'{index:02d}-{hashlib.sha256(full_name.encode()).hexdigest()[:8]}'
    directory.mkdir(parents=True, exist_ok=True)
    home = Path(os.environ['RUNNER_TEMP']) / 'q15595-case-home'
    stale = CaseProcesses(home=str(home)).cleanup()
    write_json(directory / 'prior-process-cleanup.json', stale)
    if stale['remainingPids']:
        raise RuntimeError('Previous case still has running processes')
    if home.exists():
        shutil.rmtree(home)
    recorder = Recorder(directory / 'http', full_name, evidence / 'recorder-ports.json')
    marker = uuid.uuid4().hex
    config = home / 'config'
    config.mkdir(parents=True, exist_ok=True)
    (home / 'tmp').mkdir(exist_ok=True)
    system = home / 'system.json'
    system_settings = {'context': {'fileName': 'Q15595-E2E-CONTEXT.md'}} if target else {}
    write_json(system, system_settings)
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
    if os.environ.get('LIVE_FINAL_DIAGNOSTIC') == '1':
        system_settings.update(settings)
        system_settings['skills'] = {'disabledLevels': ['bundled']}
        if index == 7:
            system_settings['tools'] = {'visible': ['cron_create', 'cron_list', 'cron_delete']}
        write_json(system, system_settings)
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
           'TB_TIMEOUT_MINUTES': '5', 'QWEN_LIVE_CASE_TOKEN': marker,
           'QWEN_LIVE_CASE_HOME': str(home),
           'OPENAI_API_KEY': 'synthetic-local-no-account', 'OPENAI_BASE_URL': recorder.base_url,
           'OPENAI_MODEL': MODEL, 'QWEN_MODEL': MODEL, 'NO_PROXY': 'localhost,127.0.0.1',
           'NODE_OPTIONS': '--require=' + str(guard),
           'QA_NETWORK_GUARD_LOG': str(directory / 'network-denials.jsonl')}
    file, *titles = full_name.split(' > ')
    title = ' '.join(titles)
    command = [shutil.which('node'), str(root / 'node_modules/vitest/vitest.mjs'),
               'run', '--root', './integration-tests', '--config', './vitest.config.ts', file,
               '-t', '^' + re.escape(title) + '$', '--fileParallelism=false', '--maxWorkers=1',
               '--retry=0', '--reporter=default', '--reporter=json',
               '--outputFile=' + str(directory / 'vitest.json')]
    write_json(directory / 'command.json', {'command': command, 'env': env,
                                           'originalCase': full_name, 'cwd': str(root),
                                           'systemSettings': system_settings,
                                           'environmentChanges': ['fresh stable HOME per case',
                                                                  'independent recorder port and directory',
                                                                  'target context filename override' if target else 'default context filename']})
    started = time.time()
    initial = resources()
    reason = None
    watchdog = 420  # Original five-minute test timeout plus startup/teardown grace.
    print(f'::group::{full_name}', flush=True)
    print('START', full_name, flush=True)
    child = None
    processes = None
    cleanup = {'observedPids': [], 'remainingPids': []}
    try:
        with (directory / 'vitest.log').open('w') as log:
            child = subprocess.Popen(command, cwd=root, env=env, stdout=log,
                                     stderr=subprocess.STDOUT, start_new_session=True)
            processes = CaseProcesses(child=child, marker=marker, home=str(home))
            while child.poll() is None:
                processes.refresh()
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
                    processes.cleanup()
                    break
                time.sleep(1)
    finally:
        if child is not None:
            if processes is None:
                processes = CaseProcesses(child=child, marker=marker, home=str(home))
            cleanup = processes.cleanup()
        write_json(directory / 'process-cleanup.json', cleanup)
        try:
            recorder.close()
        except RuntimeError as error:
            reason = 'RECORDER_CLEANUP_FAILED: ' + str(error)
        if cleanup['remainingPids']:
            reason = 'PROCESS_CLEANUP_FAILED'
    records = recorder.records
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
              'selectedCount': len(selected), 'exactOriginalCaseMatched': exact, 'selectedNames': [case['fullName'] for case in selected],
              'exitCode': child.returncode, 'seconds': time.time() - started,
              'inferenceRequests': len(inference), 'requestIds': [r['index'] for r in records],
              'modelMismatches': mismatches, 'resourceStop': reason,
              'failureMessages': selected[0].get('failureMessages', []) if exact else [],
              'evidence': str(directory), 'systemSettings': system_settings,
              'cleanup': cleanup, 'recorderUrl': recorder.base_url}
    if reason and 'CLEANUP_FAILED' in reason:
        result['status'] = 'CLEANUP_FAILED'
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


def phase_summary(sha, phase, rows, fatal=None):
    return {'sha': sha, 'phase': phase, 'attempted': len(rows),
            'originalAssertionsExecuted': sum(row.get('exactOriginalCaseMatched', False) for row in rows),
            'passed': sum(row['status'] == 'PASS' for row in rows),
            'failedOrBlocked': sum(row['status'] != 'PASS' for row in rows),
            'cases': rows, 'fatal': fatal, 'updatedAt': time.time()}


def main():
    def interrupted(signum, _frame):
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser()
    parser.add_argument('phase', choices=('rep', 'rest', 'target'))
    args = parser.parse_args()
    root = Path.cwd().resolve()
    expected = json.loads(Path(__file__).with_name('expected-cases.json').read_text())
    assert len(expected) == len(set(expected)) == 43 and REP in expected
    assert expected[14] == REP
    evidence = Path(os.environ['RUNNER_TEMP']) / 'live-evidence'
    evidence.mkdir(parents=True, exist_ok=True)
    # Stable HOME is shared across invocations only under this exclusive lock.
    lock = (evidence / 'case-execution.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
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
    if MODEL not in [entry['id'] for entry in metadata['models']['data']]:
        raise RuntimeError('The expected local model is unavailable')
    run_id = time.strftime('%Y%m%dT%H%M%S') + '-' + uuid.uuid4().hex[:8]
    rows = []
    if args.phase == 'target':
        raw = os.environ.get('LIVE_CASE_IDS', '').strip()
        values = json.loads(raw) if raw.startswith('[') else [int(value) for value in re.split(r'[,\s]+', raw) if value]
        if not values or any(type(value) is not int or not 1 <= value <= 43 for value in values):
            raise ValueError('LIVE_CASE_IDS must contain original 1-based case IDs between 1 and 43')
        ids = list(dict.fromkeys(values))
    else:
        ids = [15] if args.phase == 'rep' else [index for index in range(1, 44) if index != 15]
        if args.phase == 'rest':
            rep = json.loads((evidence / 'rep/summary.json').read_text())
            if rep['sha'] != sha or rep['passed'] != 1 or not rep['cases'][0]['liveVerification']['passed']:
                raise RuntimeError('The same-SHA representative has not passed')
            rows = list(rep['cases'])
    fatal = None
    for case_id in ids:
        parent = evidence / (f'target-{case_id}' if args.phase == 'target' else args.phase)
        run_dir = parent / 'runs' / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        write_json(run_dir / 'metadata.json', metadata)
        if args.phase == 'target' and case_id == 15:
            summary = phase_summary(sha, args.phase, [])
            summary.update({'originalCaseId': 15, 'status': 'PREVIOUS_VERIFIED_PASS_NOT_RERUN',
                            'case': REP, 'priorEvidence': 'first Ubuntu original read_file representative'})
            write_json(run_dir / 'summary.json', summary)
            write_json(parent / 'summary.json', summary)
            print('SKIP_PREVIOUS_VERIFIED_PASS', case_id, REP, flush=True)
            continue
        result = run_case(root, run_dir, expected[case_id - 1], case_id, evidence,
                          target=args.phase == 'target')
        result['originalCaseId'] = case_id
        if case_id == 4:
            result['priorDiagnosticNote'] = 'Prior formal assertion passed through its allowed prompt-timeout branch; no successful exit_plan_mode response was established.'
        rows.append(result)
        summary_rows = [result] if args.phase == 'target' else rows
        summary = phase_summary(sha, args.phase, summary_rows, result['resourceStop'])
        summary['originalCaseId'] = case_id
        summary['runId'] = run_id
        write_json(run_dir / 'summary.json', summary)
        write_json(parent / 'summary.json', summary)
        coverage_path = evidence / 'coverage.json'
        previous = json.loads(coverage_path.read_text()) if coverage_path.exists() else []
        by_name = {entry['case']: entry for entry in previous}
        by_name.update({entry['case']: entry for entry in rows})
        coverage = [by_name.get(name, {'case': name, 'status': 'NOT_STARTED',
                                     'selectedCount': 0, 'exactOriginalCaseMatched': False}) for name in expected]
        write_json(coverage_path, coverage)
        print('PROGRESS', args.phase, case_id, 'PASS', summary['passed'], flush=True)
        if result['resourceStop']:
            fatal = result['resourceStop']
            break
    aggregate = phase_summary(sha, args.phase, rows, fatal)
    write_json(evidence / f'{args.phase}-{run_id}-summary.json', aggregate)
    print('PHASE_COMPLETE', json.dumps({key: value for key, value in aggregate.items() if key != 'cases'}), flush=True)
    return 0 if not fatal and all(row['status'] == 'PASS' for row in rows) else 1


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception:
        import traceback
        traceback.print_exc()
        raise SystemExit(2)
