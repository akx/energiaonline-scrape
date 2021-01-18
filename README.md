# energiaonline-scrape

Scrapes machine-readable energy consumption data from
https://www.energiaonline.fi/ , the Turku Energia self-service portal.

## Installation

* Have a Python 3.6+ virtualenv set up.
* Install the requirements from `requirements.txt`.

## Usage

*  You can also pass in your EO username and password as the environment variables `EO_USERNAME` and `EO_PASSWORD`.
   They can also be read from an `.env` file.

### Listing distribution sites

Use this to acquire a list of distribution sites for your account; you'll need this for the other steps.

```
python -m eos -u YOURUSERNAME -p YOURPASSWORD sites
```

### Getting usage data as JSON

```
python -m eos usage -u YOURUSERNAME -p YOURPASSWORD -s SITEID -c CUSTOMERID > data.json
```

The default is to download hourly data, but you can also pass in `--resolution daily`.
By default, data for the last 30 days is downloaded.

### Updating usage data into an SQL database

Alternatively, you can update an SQL database with hourly usage information.
This is designed to run in a cronjob. Since this is backed by SQLAlchemy, you should be able
to use a MySQL or PostgreSQL database instead of Sqlite, but that hasn't quite been tested.

The schema is "described" in `eos/database.py`.

```
python -m update_database -s SITEID -c CUSTOMERID --db sqlite:///./usage.sqlite3
```
