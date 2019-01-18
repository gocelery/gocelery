# Event Types

## Task Events

* task-sent (uuid, name, args, kwargs, retries, eta, expires, queue, exchange, routing_key, root_id, parent_id)
* task-received (uuid, name, args, kwargs, retries, eta, hostname, timestamp, root_id, parent_id)
* task-started (uuid, hostname, timestamp, pid)
* task-succeeded (uuid, result, runtime, hostname, timestamp)
* task-failed (uuid, exception, traceback, hostname, timestamp)
* task-rejected (uuid, requeued)
* task-revoked (uuid, terminated, signum, expired)
* task-retried (uuid, exception, traceback, hostname, timestamp)

## Worker Events

* worker-online (hostname, timestamp, freq, sw_ident, sw_ver, sw_sys)
* worker-heartbeat (hostname, timestamp, freq, sw_ident, sw_ver, sw_sys, active, processed)
* worker-offline (hostname, timestamp, freq, sw_ident, sw_ver, sw_sys)

# Task Serialization

* json          application/json
* yaml          application/x-yaml
* pickle        application/x-python-serialize
* msgpack       application/x-msgpack
