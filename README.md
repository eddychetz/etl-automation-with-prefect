# eR2m DAILY DATA ETL AUTOMATION PORJECT

## Business Problem:

* [ ] Importing daily data from **FTP** server with **Prefect.**

## Task:

* [ ] Build an automated data import system.

## Steps:

1. Get data from **FTP** server.
2. **Clean** and **Store** data on a **CSV** file.
3. Run ETL process on every morning at **8:30 AM.**
4. Run **CRON** job on **MySQL** server to import data on a daily basis.

## GOALS

* [ ] Build a Prefect Flow.
* [ ] Add `prefect` and examine default cli logging.

### RUN COMMAND

* [ ] Activate virtual environment, run `.venv\Scripts\activate`

* [ ] Run this command `python main.py` on command line.

## GOALS

* [ ] Make a Deployment **YAML** file.
* [ ] Expose Scheduling to run the flow on an "*Internet Schedule*".

**IMPORTANT**: Interval Scheduler must be 60 seconds or greater (*it must be this minimum for it to work*).

* [ ] Can also do `cron` schedule [MOST Common Automation].

# RESOURCES

* [ ] [https://docs.prefect.io/concepts/schedules/]()
