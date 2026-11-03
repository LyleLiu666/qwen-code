"""Install the six verified public Ollama layers into the runner's model store."""
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path

source = Path(__file__).with_name("official-manifest.json")
manifest = json.loads(source.read_bytes())
models = Path(os.environ["RUNNER_TEMP"]) / "models"
blobs = models / "blobs"
blobs.mkdir(parents=True)
for layer in [manifest["config"], *manifest["layers"]]:
    digest = layer["digest"]
    assert shutil.disk_usage(models).free > layer["size"] + 16 * 1024**3
    target = blobs / digest.replace(":", "-")
    url = "https://registry.ollama.ai/v2/library/qwen3.5/blobs/" + digest
    subprocess.run(["curl", "--fail", "--silent", "--show-error", "--location", "--retry", "2", "--max-time", "600", url, "--output", str(target)], check=True)
    with target.open("rb") as stream:
        assert hashlib.file_digest(stream, "sha256").hexdigest() == digest.split(":")[1]
    assert target.stat().st_size == layer["size"]
    print("Verified", layer["mediaType"], digest, target.stat().st_size, flush=True)
tag = models / "manifests/registry.ollama.ai/library/qwen3.5/4b"
tag.parent.mkdir(parents=True)
shutil.copyfile(source, tag)
