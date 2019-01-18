
# Message Protocol v1

Ignore task message headers (only in protocol v2)

```json
"LPUSH" "celery" ""
{
  "headers": {},

  "body": "eyJleHBpcmVzIjogbnVsbCwgInV0YyI6IHRydWUsICJhcmdzIjogWzQsIDRdLCAiY2hvcmQiOiBudWxsLCAiY2FsbGJhY2tzIjogbnVsbCwgImVycmJhY2tzIjogbnVsbCwgInRhc2tzZXQiOiBudWxsLCAiaWQiOiAiYTM5MGYzZGEtNDY3Zi00Mzc4LTgzODAtMGM2NmMzYzc3M2IwIiwgInJldHJpZXMiOiAwLCAidGFzayI6ICJ0YXNrcy5hZGQiLCAiZ3JvdXAiOiBudWxsLCAidGltZWxpbWl0IjogW251bGwsIG51bGxdLCAiZXRhIjogbnVsbCwgImt3YXJncyI6IHt9fQ==",

  "content-type": "application/json",
  "content-encoding": "utf-8",

  "properties": {
    "priority": 0,
    "body_encoding": "base64",
    "correlation_id": "a390f3da-467f-4378-8380-0c66c3c773b0",
    "reply_to": "36cd3948-0af1-3bb8-ac52-0dd344254ae2",
    "delivery_info": {
      "routing_key": "celery",
      "exchange": ""
    },
    "delivery_mode": 2,
    "delivery_tag": "0afd68a5-df67-4a40-9cdf-39b1a41120ca"
  },

}

// body
{
  "task": "tasks.add",
  "id": "a390f3da-467f-4378-8380-0c66c3c773b0",
  "args": [4,4],
  "kwargs": {},
  "retries": 0,
  "eta": null,
  "expires": null,
  "utc": true,
  "chord": null,
  "callbacks": null,
  "errbacks": null,
  "taskset": null,
  "group": null,
  "timelimit": [null,null],
}
```

```json
"GET" "celery-task-meta-a390f3da-467f-4378-8380-0c66c3c773b0"
```

```json
"SETEX" "celery-task-meta-a390f3da-467f-4378-8380-0c66c3c773b0" "86400" ""
{
  "status": "SUCCESS",
  "traceback": null,
  "result": 8,
  "task_id": "a390f3da-467f-4378-8380-0c66c3c773b0",
  "children": []
}
```

```json
"PUBLISH" "celery-task-meta-a390f3da-467f-4378-8380-0c66c3c773b0" ""
{
  "status": "SUCCESS",
  "traceback": null,
  "result": 8,
  "task_id": "a390f3da-467f-4378-8380-0c66c3c773b0",
  "children": []
}
```




```json
"PUBLISH" "/0.celeryev/worker.heartbeat" ""
{
  "body": "eyJzd19zeXMiOiAiRGFyd2luIiwgImNsb2NrIjogOCwgInRpbWVzdGFtcCI6IDE1NDc3ODA3OTQuMTk2OTAxLCAiaG9zdG5hbWUiOiAiY2VsZXJ5QG9wIiwgInBpZCI6IDEzMjQ4LCAic3dfdmVyIjogIjQuMi4xIiwgInV0Y29mZnNldCI6IDUsICJsb2FkYXZnIjogWzEuNzksIDIuMCwgMS45Nl0sICJwcm9jZXNzZWQiOiAwLCAiYWN0aXZlIjogMCwgImZyZXEiOiAyLjAsICJ0eXBlIjogIndvcmtlci1oZWFydGJlYXQiLCAic3dfaWRlbnQiOiAicHktY2VsZXJ5In0=",

  "headers": {
    "hostname": "celery@op"
  },

  "content-type": "application/json",

  "properties": {
    "priority": 0,
    "body_encoding": "base64",
    "delivery_info": {
      "routing_key": "worker.heartbeat",
      "exchange": "celeryev"
    },
    "delivery_mode": 1,
    "delivery_tag": "8708448e-2732-4eb8-a43a-a05e01c4f21e"
  },

  "content-encoding": "utf-8"
}

// body
{"sw_sys": "Darwin", "clock": 8, "timestamp": 1547780794.196901, "hostname": "celery@op", "pid": 13248, "sw_ver": "4.2.1", "utcoffset": 5, "loadavg": [1.79, 2.0, 1.96], "processed": 0, "active": 0, "freq": 2.0, "type": "worker-heartbeat", "sw_ident": "py-celery"}
```
