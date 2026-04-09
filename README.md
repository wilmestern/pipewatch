# pipewatch

A lightweight CLI tool for monitoring and alerting on data pipeline health metrics across multiple sources.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git && cd pipewatch && pip install .
```

---

## Usage

Define your pipeline sources in a config file (`pipewatch.yaml`), then run:

```bash
pipewatch monitor --config pipewatch.yaml
```

**Example config:**

```yaml
pipelines:
  - name: sales_etl
    source: postgres
    check_interval: 60
    alert_on:
      - row_count_drop: 20%
      - last_run_delay: 10m

  - name: event_stream
    source: kafka
    check_interval: 30
    alert_on:
      - lag_threshold: 5000
```

**Run a one-time health check:**

```bash
pipewatch check --pipeline sales_etl
```

**View status of all monitored pipelines:**

```bash
pipewatch status
```

Output:

```
PIPELINE        STATUS    LAST RUN     ALERTS
sales_etl       ✓ OK      2m ago       None
event_stream    ✗ WARN    8m ago       Lag > 5000
```

---

## License

This project is licensed under the [MIT License](LICENSE).