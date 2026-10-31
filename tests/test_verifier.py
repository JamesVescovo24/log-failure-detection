import hashlib
import pytest
from contract import BatchInput,SaveInput,SaveOutput,JobStatus
from plugin.logic import pin_reference,read_dataset,save_candidate
from plugin.storage import REPORT_PATH
from verifier import verify_input,verify_output,verify_task

@pytest.mark.parametrize('batch',['retries','edge','all-good'])
def test_gold(tmp_path,batch):
    task=BatchInput(batch);ref=pin_reference(task,tmp_path)
    assert verify_output('pin_log_truth',task,ref,str(tmp_path))['valid']
    assert verify_output('read_job_logs',task,read_dataset(task,tmp_path),str(tmp_path))['valid']
    candidate=SaveInput(batch,ref.expected,ref.failed)
    output=save_candidate(candidate,tmp_path)
    assert verify_output('save_job_statuses',candidate,output,str(tmp_path))['valid']
    assert verify_task('detect-failed-jobs',task,output,ref,str(tmp_path))['valid']

@pytest.mark.parametrize('variant',['wrong_status','missing','duplicate','extra','missing_failure','extra_failure','duplicate_failure'])
def test_bad_candidate(tmp_path,variant):
    task=BatchInput('retries');ref=pin_reference(task,tmp_path)
    statuses=list(ref.expected);failed=list(ref.failed)
    if variant=='wrong_status': statuses[0]=JobStatus(statuses[0].job_id,'failed')
    elif variant=='missing': statuses.pop()
    elif variant=='duplicate': statuses.append(statuses[0])
    elif variant=='extra': statuses.append(JobStatus('ghost','succeeded'))
    elif variant=='missing_failure': failed.pop()
    elif variant=='extra_failure': failed.append('D')
    elif variant=='duplicate_failure': failed.append(failed[0])
    output=save_candidate(SaveInput('retries',statuses,failed),tmp_path)
    assert not verify_task('detect-failed-jobs',task,output,ref,str(tmp_path))['valid']

def test_missing_and_malformed(tmp_path):
    task=BatchInput('all-good');ref=pin_reference(task,tmp_path)
    output=save_candidate(SaveInput('all-good',ref.expected,ref.failed),tmp_path)
    (tmp_path/REPORT_PATH).write_text('{oops')
    output.digest=hashlib.sha256(b'{oops').hexdigest()
    assert not verify_task('detect-failed-jobs',task,output,ref,str(tmp_path))['valid']
    (tmp_path/REPORT_PATH).unlink()
    assert not verify_task('detect-failed-jobs',task,output,ref,str(tmp_path))['valid']

def test_unknown_batch_and_ref_corruption(tmp_path):
    assert not verify_input('read_job_logs',BatchInput('unknown'))['valid']
    task=BatchInput('edge');ref=pin_reference(task,tmp_path)
    output=save_candidate(SaveInput('edge',ref.expected,ref.failed),tmp_path)
    ref.expected[0].status='failed' if ref.expected[0].status!='failed' else 'succeeded'
    assert not verify_task('detect-failed-jobs',task,output,ref,str(tmp_path))['valid']
