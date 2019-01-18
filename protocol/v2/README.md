
# Message Protocol v2

Protocol version detected by the presence of a task message header

```json
"LPUSH" "celery" ""
{
  "body": "W1s0LCA0XSwge30sIHsiY2hvcmQiOiBudWxsLCAiY2FsbGJhY2tzIjogbnVsbCwgImVycmJhY2tzIjogbnVsbCwgImNoYWluIjogbnVsbH1d",
  "headers": {
    "id": "95a5dbf9-3fe9-44b6-8fca-8ea697125388",
    "lang": "py",
    "task": "tasksv2.add",
    "root_id": "95a5dbf9-3fe9-44b6-8fca-8ea697125388",
    "parent_id": null,
    "group": null,

    "origin": "gen15620@op",
    "expires": null,
    "shadow": null,
    "kwargsrepr": "{}",
    "retries": 0,
    "timelimit": [
      null,
      null
    ],

    "argsrepr": "(4, 4)",
    "eta": null
  },
  "content-type": "application/json",
  "properties": {
    "priority": 0,
    "body_encoding": "base64",

    "correlation_id": "95a5dbf9-3fe9-44b6-8fca-8ea697125388",
    "reply_to": "990b3839-5c61-3888-8c80-710f61798427",

    "delivery_info": {
      "routing_key": "celery",
      "exchange": ""
    },
    "delivery_mode": 2,
    "delivery_tag": "15c08ea5-8e47-4327-8246-b31cb3ce27d9"
  },
  "content-encoding": "utf-8"
}

// body
[
  [
    4,
    4
  ],
  {},
  {
    "chord": null,
    "callbacks": null,
    "errbacks": null,
    "chain": null
  }
]
```

```json
"HSET" "unacked" "15c08ea5-8e47-4327-8246-b31cb3ce27d9" ""
[
  {
    "body": "W1s0LCA0XSwge30sIHsiY2hvcmQiOiBudWxsLCAiY2FsbGJhY2tzIjogbnVsbCwgImVycmJhY2tzIjogbnVsbCwgImNoYWluIjogbnVsbH1d",
    "headers": {
      "origin": "gen15620@op",
      "lang": "py",
      "task": "tasksv2.add",
      "group": null,
      "root_id": "95a5dbf9-3fe9-44b6-8fca-8ea697125388",
      "expires": null,
      "retries": 0,
      "timelimit": [
        null,
        null
      ],
      "argsrepr": "(4, 4)",
      "eta": null,
      "parent_id": null,
      "shadow": null,
      "id": "95a5dbf9-3fe9-44b6-8fca-8ea697125388",
      "kwargsrepr": "{}"
    },
    "content-type": "application/json",
    "properties": {
      "body_encoding": "base64",
      "delivery_info": {
        "routing_key": "celery",
        "exchange": ""
      },
      "delivery_mode": 2,
      "priority": 0,
      "correlation_id": "95a5dbf9-3fe9-44b6-8fca-8ea697125388",
      "reply_to": "990b3839-5c61-3888-8c80-710f61798427",
      "delivery_tag": "15c08ea5-8e47-4327-8246-b31cb3ce27d9"
    },
    "content-encoding": "utf-8"
  },
  "",
  "celery"
]

// body
[
  [
    4,
    4
  ],
  {},
  {
    "chord": null,
    "callbacks": null,
    "errbacks": null,
    "chain": null
  }
]
```

```json
"GET" "celery-task-meta-95a5dbf9-3fe9-44b6-8fca-8ea697125388"
```

```json
"SETEX" "celery-task-meta-95a5dbf9-3fe9-44b6-8fca-8ea697125388" "86400" ""
{
  "status": "SUCCESS",
  "traceback": null,
  "result": 8,
  "task_id": "95a5dbf9-3fe9-44b6-8fca-8ea697125388",
  "children": []
}
```

```json
"PUBLISH" "celery-task-meta-95a5dbf9-3fe9-44b6-8fca-8ea697125388" ""
{
  "status": "SUCCESS",
  "traceback": null,
  "result": 8,
  "task_id": "95a5dbf9-3fe9-44b6-8fca-8ea697125388",
  "children": []
}
```

```
"UNSUBSCRIBE" "celery-task-meta-95a5dbf9-3fe9-44b6-8fca-8ea697125388"
```