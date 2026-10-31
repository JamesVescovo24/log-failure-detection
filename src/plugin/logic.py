"""Read logs, compute hidden ground truth and save a candidate artifact."""
from pathlib import Path
from dataclasses import asdict
from contract import BatchInput, Event, JobStatus, ReadOutput, ReferenceOutput, SaveInput, SaveOutput
from plugin.datasets import FIXTURES, RULES
from plugin.storage import store

STATUS={'QUEUED':'queued','START':'running','ERROR':'failed','SUCCESS':'succeeded'}

def _dataset(batch):
    if batch not in FIXTURES:
        raise ValueError('unknown event batch; choose retries, edge, all-good')
    data=FIXTURES[batch]
    return list(data['jobs']), [Event(**ev) for ev in data['events']]

def read_dataset(given: BatchInput, workspace: Path) -> ReadOutput:
    jobs,events=_dataset(given.batch)
    return ReadOutput(given.batch, jobs, events, RULES)

def pin_reference(given: BatchInput, workspace: Path) -> ReferenceOutput:
    jobs,events=_dataset(given.batch)
    expected=[]
    for job in jobs:
        found=sorted((event for event in events if event.job_id==job),key=lambda ev:(ev.timestamp,ev.sequence))
        expected.append(JobStatus(job,STATUS[found[-1].event] if found else 'missing'))
    failed=[x.job_id for x in expected if x.status in ('failed','running','missing')]
    return ReferenceOutput(given.batch,jobs,events,expected,failed)

def save_candidate(given: SaveInput, workspace: Path) -> SaveOutput:
    _dataset(given.batch)
    if len(given.statuses)>500 or len(given.failed)>500:
        raise ValueError('too many jobs')
    data={'batch':given.batch,'statuses':[asdict(s) for s in given.statuses],'failed':list(given.failed)}
    path,digest=store(workspace,data)
    return SaveOutput(given.batch,path,digest,len(given.statuses))
