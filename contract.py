"""Structured event-log failure detection task and tool contracts."""
from dataclasses import dataclass
from enum import Enum
from typing import Annotated
from slimconfig import Output

class Tool(Enum):
    PIN = "pin_log_truth"
    READ = "read_job_logs"
    SAVE = "save_job_statuses"

class Task(Enum):
    MAIN = "detect-failed-jobs"

@dataclass
class BatchInput(Output):
    batch: Annotated[str, "Event batch: retries, edge, or all-good"]

@dataclass
class Event(Output):
    job_id: str
    timestamp: str
    sequence: int
    event: Annotated[str, "QUEUED, START, ERROR or SUCCESS"]

@dataclass
class JobStatus(Output):
    job_id: str
    status: Annotated[str, "One of queued, running, failed, succeeded or missing"]

@dataclass
class ReadOutput(Output):
    batch: str
    jobs: list[str]
    events: list[Event]
    rules: str

@dataclass
class ReferenceOutput(Output):
    batch: str
    jobs: list[str]
    events: list[Event]
    expected: list[JobStatus]
    failed: list[str]

@dataclass
class SaveInput(Output):
    batch: str
    statuses: Annotated[list[JobStatus], "A status for every registered job; order irrelevant"]
    failed: Annotated[list[str], "Every job with final state running, failed, or missing (order irrelevant)"]

@dataclass
class SaveOutput(Output):
    batch: str
    path: str
    digest: str
    count: int

TOOLS = {Tool.PIN: (BatchInput, ReferenceOutput), Tool.READ: (BatchInput, ReadOutput), Tool.SAVE: (SaveInput, SaveOutput)}
TASKS = {Task.MAIN: (BatchInput, SaveOutput)}
