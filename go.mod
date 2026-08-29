module github.com/domainry/domainry-scheduler

go 1.26.0

require (
	github.com/domainry/domainry-scheduler-sdk v0.0.0-00010101000000-000000000000
	github.com/robfig/cron/v3 v3.0.1
)

replace github.com/domainry/domainry-scheduler-sdk => ../domainry-scheduler-sdk
