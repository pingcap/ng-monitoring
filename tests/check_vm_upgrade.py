"""Check forward upgrades of persisted data, queries, and snapshots across NGM binaries.

Build the release baseline and upgrade separately, then run:
    python3 tests/check_vm_upgrade.py --old-binary /tmp/ngm-old --new-binary /tmp/ngm-new

Uses disposable local storage and loopback HTTP listeners; no PD cluster is needed.
Leaves logs, disposable storage, and result.json in a temporary directory.
"""

import argparse
import contextlib
from datetime import datetime, timezone
import json
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--old-binary', required=True, type=Path)
parser.add_argument('--new-binary', required=True, type=Path)
args = parser.parse_args()
args.old_binary = args.old_binary.resolve(strict=True)
args.new_binary = args.new_binary.resolve(strict=True)
root = Path(tempfile.mkdtemp(prefix='ng-monitoring-vm-compat-'))
data = root / 'data'
stamp = int(time.time())
previous_month = int(datetime.fromtimestamp(stamp, timezone.utc).replace(
    day=1, hour=0, minute=0, second=0, microsecond=0).timestamp()) - 1
results = {'directory': str(root), 'old_binary': str(args.old_binary),
           'new_binary': str(args.new_binary), 'checks': []}
metrics = ['cpu_time', 'network_in_bytes', 'network_out_bytes',
           'logical_read_bytes', 'logical_write_bytes', 'rocksdb_block_read_count',
           'sql_exec_count']


def request(base, path, params=None, body=None):
    url = base + path
    if params:
        url += '?' + urllib.parse.urlencode(params, doseq=True)
    req = urllib.request.Request(url, data=body)
    with urllib.request.urlopen(req, timeout=30) as response:
        return response.read().decode()


@contextlib.contextmanager
def server(binary, label):
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    logdir = root / label
    logdir.mkdir()
    config = root / (label + '.toml')
    config.write_text(f'''address = "127.0.0.1:{port}"
advertise-address = "127.0.0.1:{port}"
[storage]
path = "{data}"
docdb-backend = "sqlite"
[log]
path = "{logdir}"
level = "INFO"
[pd]
endpoints = ["127.0.0.1:1"]
[tsdb]
memory-allowed-bytes = 67108864
''')
    with (logdir / 'stdout.log').open('w') as output:
        process = subprocess.Popen([binary, '--config', str(config)], stdout=output,
                                   stderr=subprocess.STDOUT)
        try:
            base = f'http://127.0.0.1:{port}'
            for _ in range(300):
                if process.poll() is not None:
                    raise RuntimeError(f'{label} exited early: {process.returncode}; see {logdir}')
                try:
                    if json.loads(request(base, '/health')).get('health'):
                        break
                except (OSError, ValueError):
                    time.sleep(0.1)
            else:
                raise RuntimeError(f'{label} failed to become ready; see {logdir}')
            yield base
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise RuntimeError(f'{label} did not stop cleanly')
            if process.returncode != 0:
                raise RuntimeError(f'{label} exit status {process.returncode}; see {logdir}')


def export(base):
    text = request(base, '/api/v1/export', {'match[]': '{instance="vm-upgrade-check"}',
                   'start': previous_month - 60, 'end': stamp + 60})
    samples = []
    for line in text.splitlines():
        row = json.loads(line)
        labels = tuple(sorted(row['metric'].items()))
        samples.extend((labels, ts, value) for ts, value in zip(row['timestamps'], row['values']))
    return sorted(samples)


def import_samples(base, digests, timestamps):
    lines = []
    expected = []
    for metric in metrics:
        for idx, digest in enumerate(digests, 1):
            labels = {'__name__': metric, 'instance': 'vm-upgrade-check',
                      'instance_type': 'tikv', 'sql_digest': digest, 'plan_digest': 'dd',
                      'table': 't1'}
            selector = ','.join(f'{key}={json.dumps(value)}' for key, value in labels.items()
                                if key != '__name__')
            for offset, ts in enumerate(timestamps, 1):
                value = idx * 10 + offset
                lines.append(f'{metric}{{{selector}}} {value} {ts * 1000}')
                expected.append((tuple(sorted(labels.items())), ts * 1000, value))
    request(base, '/api/v1/import/prometheus', body=('\n'.join(lines) + '\n').encode())
    request(base, '/internal/force_flush')
    return sorted(expected)


def queries(base):
    expressions = [
        'sum(sum_over_time(cpu_time{instance="vm-upgrade-check"}[60s])) by (sql_digest)',
        'topk(2,sum(sum_over_time(network_in_bytes{instance="vm-upgrade-check"}[60s])) by (sql_digest))',
        'sum(sum_over_time(logical_read_bytes{instance="vm-upgrade-check"}[60s]))',
    ]
    outputs = []
    for expr in expressions:
        response = json.loads(request(base, '/api/v1/query',
                         {'query': expr, 'time': stamp - 95, 'nocache': '1'}))
        assert response['status'] == 'success', response
        assert response['data']['result'], response
        outputs.append(sorted(response['data']['result'], key=lambda row: json.dumps(row['metric'], sort_keys=True)))
    response = json.loads(request(base, '/api/v1/query_range',
                     {'query': expressions[0], 'start': stamp - 120,
                      'end': stamp - 100, 'step': '10s', 'nocache': '1'}))
    assert response['status'] == 'success', response
    outputs.append(sorted(response['data']['result'], key=lambda row: json.dumps(row['metric'], sort_keys=True)))
    return outputs


def check(message):
    results['checks'].append(message)
    print(message, flush=True)


try:
    with server(str(args.old_binary), 'old-write') as base:
        old_samples = import_samples(base, ['aa', 'bb', 'cc'],
                                    [previous_month, stamp - 120, stamp - 110, stamp - 100])
        assert export(base) == old_samples
        old_queries = queries(base)
        snapshot = json.loads(request(base, '/snapshot/create'))
        assert snapshot['status'] == 'ok', snapshot
        snapshot_name = snapshot['snapshot']
        check(f'baseline: wrote and read {len(old_samples)} samples across two monthly partitions; snapshot {snapshot_name}')

    with server(str(args.new_binary), 'new-read-write') as base:
        assert export(base) == old_samples, 'new binary cannot read all old samples'
        assert queries(base) == old_queries, 'query results changed on the old fixture'
        snapshots = json.loads(request(base, '/snapshot/list'))
        assert snapshot_name in snapshots['snapshots'], snapshots
        check('upgrade: all old raw samples, instant/range/TopK/IO queries, and old snapshot remain accessible')
        new_samples = import_samples(base, ['aa', 'ee'], [stamp - 90])
        all_samples = sorted(old_samples + new_samples)
        assert export(base) == all_samples
        snapshot = json.loads(request(base, '/snapshot/create'))
        assert snapshot['status'] == 'ok', snapshot
        new_snapshot_name = snapshot['snapshot']
        check(f'upgrade: appended {len(new_samples)} samples to existing and new series; snapshot creation passed')

    with server(str(args.new_binary), 'new-restart') as base:
        assert export(base) == all_samples
        snapshots = json.loads(request(base, '/snapshot/list'))
        assert snapshot_name in snapshots['snapshots'], snapshots
        assert new_snapshot_name in snapshots['snapshots'], snapshots
        check('upgraded restart: all old and new samples remain readable')
except Exception as error:
    results['error'] = repr(error)
    print(repr(error), flush=True)
    raise
finally:
    (root / 'result.json').write_text(json.dumps(results, indent=2))
    print('Evidence: ' + str(root / 'result.json'), flush=True)
