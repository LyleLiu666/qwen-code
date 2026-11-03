#!/usr/bin/env bash
set -euo pipefail
group=$1
ids=$2
warm=$3
h=../harness/ci-harness
if [[ -f "$RUNNER_TEMP/halt-more-cases" ]]; then
  echo 'Previous group encountered an infrastructure blocker.'
  exit 1
fi
if [[ "$warm" != "-" ]]; then
  bash "$h/run-local.sh" curl -fsS --max-time 1100 -H 'Content-Type: application/json' --data-binary "@$h/warm/$warm.json" http://127.0.0.1:11434/v1/chat/completions > "$RUNNER_TEMP/live-evidence/warm-$group.json"
fi
set +e
bash "$h/run-local.sh" env LIVE_CASE_IDS="$ids" python3 -u "$h/run-live.py" target
rc=$?
set -e
python3 - "$group" "$ids" "$rc" <<'PY'
import json, os, sys
from pathlib import Path
group, ids, rc = sys.argv[1:]
root = Path(os.environ['RUNNER_TEMP'])
directory = root / 'live-evidence'
cases = [json.loads(p.read_text()) for p in directory.glob('target-*/**/result.json')]
rows = [{k: r.get(k) for k in ('case','status','selectedNames','seconds','inferenceRequests','resourceStop','cleanup','failureMessages')} for r in cases]
out = {'group': group, 'targetIds': ids, 'exitCode': int(rc), 'cumulativeResults': rows}
(directory / f'checkpoint-{group}.json').write_text(json.dumps(out,indent=2)+'\n')
responses = [r for p in directory.glob('target-*/**/requests.json') for r in json.loads(p.read_text())]
no_pilot_response = group == 'pilot' and not any(r.get('completed') and r.get('httpStatus') == 200 for r in responses)
if any(r['resourceStop'] for r in rows) or not cases or no_pilot_response:
    (root/'halt-more-cases').touch()
PY
exit "$rc"
