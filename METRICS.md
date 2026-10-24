# Metrics

Enable the Prometheus endpoint by adding this at the top level of your TOML config
(before any `[section]`):

```toml
metrics_listen = "127.0.0.1:9090"
```

The endpoint is disabled when this option is omitted. It starts before Matrix login
completes. Fetch it with:

```sh
curl http://127.0.0.1:9090/metrics
```

For container scraping, bind to `0.0.0.0:9090` and publish the port as appropriate.
There is no authentication or TLS on this endpoint; restrict access to your
monitoring network.

## Gauges

All metrics have a `bridge` label using the configured bridge name.

- `bridge_matrix_last_success_age_seconds`: seconds since the last successful
  Matrix sync, including an initial backlog-skipping sync. This is connection
  health, not room membership or message-delivery health. All bridges share the
  same Matrix client and therefore the same value.
- `bridge_irc_last_success_age_seconds`: seconds since the last received IRC
  server message, including PING/PONG. Bridges on the same IRC network share
  the same value. Quiet connections can show increasing age until the next
  heartbeat. Sending a message or opening a socket alone does not reset it.
- `bridge_pending_message_age_seconds`: age since enqueue of the oldest pending
  message for that bridge and `destination` (`irc` or `matrix`). `irc` means
  Matrix → IRC; `matrix` means IRC → Matrix. Includes delayed retries; excludes
  delivered and permanently failed messages. Zero means no pending messages.

Connection ages are process-local, increase during outages, and are `+Inf` until
that connection has first been observed healthy in the current process. Normal
Matrix long polling and IRC heartbeat intervals must be allowed for in alerts.
Queue ages are read from SQLite at scrape time and survive restarts. If SQLite
cannot be read, the scrape returns HTTP 500 rather than misleading zero values.
Ages are nonnegative seconds and may include fractions.

## Example response

Content-Type: `text/plain; version=0.0.4; charset=utf-8`

```text
# HELP bridge_matrix_last_success_age_seconds Seconds since the last successful Matrix sync; +Inf until first success.
# TYPE bridge_matrix_last_success_age_seconds gauge
bridge_matrix_last_success_age_seconds{bridge="rust"} 3.25
# HELP bridge_irc_last_success_age_seconds Seconds since the last received IRC server message; +Inf until first success.
# TYPE bridge_irc_last_success_age_seconds gauge
bridge_irc_last_success_age_seconds{bridge="rust"} 12.5
# HELP bridge_pending_message_age_seconds Age of the oldest pending message since enqueue, including retries; zero if empty.
# TYPE bridge_pending_message_age_seconds gauge
bridge_pending_message_age_seconds{bridge="rust",destination="irc"} 8.75
bridge_pending_message_age_seconds{bridge="rust",destination="matrix"} 0
```
