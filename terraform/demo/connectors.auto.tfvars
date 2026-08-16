# Non-secret operational config — toggle brokers by editing the lists below.
# Decision: docs/adr/0107-move-orchestrator-config-to-committed-auto-tfvars.md
scheduled               = false
schedule_cron           = "cron(0 6 * * ? *)"
schedule_connectors     = ["ibkr", "trading212"]
file_arrival_connectors = ["ibkr", "trading212", "xtb"]