"""Deliberately unordered event streams; same timestamps use sequence to break ties."""
def e(job, stamp, seq, event):
    return {"job_id":job,"timestamp":"2026-10-01T00:"+stamp+"Z","sequence":seq,"event":event}

FIXTURES = {
    "retries": {
        "jobs": ["A", "B", "C", "D", "E", "F"],
        "events": [
            e('B','05:00',8,'ERROR'),e('A','01:00',2,'START'),e('E','10:00',15,'START'),
            e('D','06:00',11,'QUEUED'),e('A','02:00',3,'ERROR'),e('B','04:00',7,'START'),
            e('C','08:00',10,'START'),e('A','00:00',1,'QUEUED'),e('A','03:00',4,'START'),
            e('A','04:00',5,'SUCCESS'),e('B','03:00',6,'QUEUED'),e('E','06:00',12,'QUEUED'),
            e('E','07:00',13,'START'),e('E','09:00',14,'SUCCESS'),e('C','07:00',9,'QUEUED'),
        ],
    },
    "edge": {
        "jobs": ["Q", "T", "S", "M"],
        "events": [
            e('T','05:00',8,'SUCCESS'),e('S','07:00',10,'SUCCESS'),e('Q','02:00',3,'QUEUED'),
            e('T','05:00',7,'ERROR'),e('S','08:00',11,'ERROR'),e('T','01:00',1,'START'),
            e('S','06:00',9,'START'),
        ],
    },
    "all-good": {
        "jobs": ["Z", "Y"],
        "events": [e('Y','03:00',4,'SUCCESS'),e('Z','01:00',2,'SUCCESS'),e('Y','02:00',3,'START'),e('Z','00:00',1,'START')],
    },
}
RULES = (
    'For each registered job, sort its events by (timestamp, sequence), not list order. The latest event '
    'determines status: QUEUED=queued, START=running, ERROR=failed, SUCCESS=succeeded. '
    'Jobs with no events are missing. Report exactly one status for every registered job. '
    'failed must contain all and only jobs with final status failed, running or missing; '
    'jobs queued but never started are not failures. Compare IDs as sets, not by output order.'
)
