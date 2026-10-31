"""Verifies outcome by reconstructing final job states from pinned event stream."""
from contract import Tool
from .checks import result, read_receipt
BATCHES=('retries','edge','all-good')
KINDS={'QUEUED':'queued','START':'running','ERROR':'failed','SUCCESS':'succeeded'}
FAIL={'running','failed','missing'}

def replay(jobs,events):
    if len(set(jobs)) != len(jobs) or any(not isinstance(job,str) or not job for job in jobs):
        return None
    sequence=set()
    grouped={job:[] for job in jobs}
    for e in events:
        if e.job_id not in grouped or e.event not in KINDS or type(e.sequence) is not int or e.sequence in sequence:
            return None
        if not isinstance(e.timestamp,str) or len(e.timestamp) != 20 or not e.timestamp.endswith('Z'):
            return None
        sequence.add(e.sequence)
        grouped[e.job_id].append(e)
    return {job:(KINDS[sorted(grouped[job], key=lambda e:(e.timestamp,e.sequence))[-1].event] if grouped[job] else 'missing') for job in jobs}

def as_map(items):
    out={}
    for x in items:
        if not isinstance(x,dict) or set(x) != {'job_id','status'}:
            return None
        if not isinstance(x['job_id'],str) or not isinstance(x['status'],str) or x['job_id'] in out:
            return None
        out[x['job_id']]=x['status']
    return out

def verify_input(tool,value):
    ok=value.batch in BATCHES
    if tool == Tool.SAVE:
        ok=ok and len(value.statuses) <= 500 and len(value.failed) <= 500
    return result(ok,'input accepted' if ok else 'unknown batch or too many jobs')

def verify_output(tool,input_value,output_value,workspace):
    if tool in (Tool.PIN,Tool.READ):
        expected=replay(output_value.jobs,output_value.events)
        ok=output_value.batch==input_value.batch and expected is not None
        if tool == Tool.PIN and ok:
            statuses=as_map([{'job_id':x.job_id,'status':x.status} for x in output_value.expected])
            ok=statuses==expected and set(output_value.failed)=={j for j,s in expected.items() if s in FAIL} and len(output_value.failed)==len(set(output_value.failed))
        return result(ok,'pinned records valid' if ok else 'bad events or inconsistent reference')
    saved,problem=read_receipt(workspace,output_value)
    original={'batch':input_value.batch,'statuses':[{'job_id':x.job_id,'status':x.status} for x in input_value.statuses], 'failed':input_value.failed}
    ok=not problem and saved==original and output_value.batch==input_value.batch and output_value.count==len(input_value.statuses)
    return result(ok,'log report saved' if ok else problem or 'stored report differs from supplied candidate')

def verify_task(arguments,output,reference,workspace):
    if reference is None or reference.batch!=arguments.batch or output.batch!=arguments.batch:
        return result(False,'missing or mismatched pinned log batch')
    expected=replay(reference.jobs,reference.events)
    if expected is None:
        return result(False,'invalid pinned event stream')
    pinned=as_map([{'job_id':x.job_id,'status':x.status} for x in reference.expected])
    if pinned!=expected or set(reference.failed)!={j for j,s in expected.items() if s in FAIL} or len(reference.failed)!=len(set(reference.failed)):
        return result(False,'pinned expected states disagree with events')
    saved,problem=read_receipt(workspace,output)
    if problem:
        return result(False,problem)
    if saved.get('batch')!=arguments.batch or not isinstance(saved.get('statuses'),list) or not isinstance(saved.get('failed'),list):
        return result(False,'invalid report shape or batch')
    statuses=as_map(saved['statuses'])
    if statuses!=expected or output.count!=len(saved['statuses']):
        return result(False,'incorrect, omitted, duplicate or invented job status',expected=expected)
    failed=saved['failed']
    if any(not isinstance(job,str) for job in failed) or len(failed)!=len(set(failed)) or set(failed)!={j for j,s in expected.items() if s in FAIL}:
        return result(False,'failed-job IDs do not equal running, failed and missing jobs')
    return result(True,'every final job state and failed ID independently verified',jobs=len(expected),failures=len(failed))
