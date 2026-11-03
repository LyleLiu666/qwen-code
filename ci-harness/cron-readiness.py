"""Warm the new public cron schemas through an ordinary neutral candidate CLI request."""
from pathlib import Path
import os, json, subprocess
root=Path.cwd()
tmp=Path(os.environ['RUNNER_TEMP'])
home=tmp/'q15595-case-home'; config=home/'config';config.mkdir(parents=True,exist_ok=True)
fixture=root/'.integration-tests/neutral-readiness/workspace'
fixture.mkdir(parents=True, exist_ok=True)
settings={'context':{'fileName':'Q15595-E2E-CONTEXT.md'},'skills':{'disabledLevels':['bundled']},
          'tools':{'visible':['cron_create','cron_list','cron_delete']},
          'security':{'auth':{'selectedType':'openai'},'folderTrust':{'enabled':False}},
          'model':{'name':'qwen3.5:4b','baseUrl':'http://127.0.0.1:11434/v1','skipNextSpeakerCheck':True},
          'modelProviders':{'openai':[{'id':'qwen3.5:4b','envKey':'OPENAI_API_KEY','baseUrl':'http://127.0.0.1:11434/v1',
          'generationConfig':{'contextWindowSize':32768,'timeout':850000,'maxRetries':0,'samplingParams':{'temperature':0,'max_tokens':2048},'extra_body':{'reasoning_effort':'none'}}}]},
          'memory':{'enableManagedAutoMemory':False,'enableManagedAutoDream':False}}
f=home/'system.json';f.write_text(json.dumps(settings)); (config/'settings.json').write_text('{}')
env=dict(os.environ,HOME=str(home),USERPROFILE=str(home),QWEN_HOME=str(config),QWEN_CODE_SYSTEM_SETTINGS_PATH=str(f),QWEN_CODE_SYSTEM_DEFAULTS_PATH=str(f),OPENAI_API_KEY='synthetic-local',OPENAI_BASE_URL='http://127.0.0.1:11434/v1',OPENAI_MODEL='qwen3.5:4b',QWEN_SANDBOX='false',QWEN_CODE_MODELS_DEV_REFRESH='off')
with (tmp/'live-evidence/cron-neutral.log').open('w') as log:
 subprocess.run(['node',str(root/'dist/cli.js'),'--no-chat-recording','--yolo','--prompt','Reply READY. There is no task to perform.'],cwd=fixture,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=1100,check=True)
