.PHONY: webserver
webserver:
	airflow webserver

.PHONY: scheduler
scheduler:
	airflow scheduler