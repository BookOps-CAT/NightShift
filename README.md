![tests](https://github.com/BookOps-CAT/NightShift/actions/workflows/tests.yaml/badge.svg?branch=main) [![Coverage Status](https://coveralls.io/repos/github/BookOps-CAT/NightShift/badge.svg?branch=main)](https://coveralls.io/github/BookOps-CAT/NightShift?branch=main) [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# NightShift
Copy cataloging bot.


 [![Overview](https://github.com/BookOps-CAT/NightShift/blob/main/docs/media/nightshift-overview.png)](https://github.com/BookOps-CAT/NightShift/blob/main/docs/media/nightshift-overview.png)


## Version
> 0.1.0

## Local Installation & Usage

### Prerequisits
1. PostgresSQL
	1. download and install [PostgreSQL server](https://www.postgresql.org/download/) 
	2. create new database 
2. Create a configuration file 
	1. follow `nightshift/config/config.yaml.example` to provide all required credentials 
3. Export Sierra brief bibs for enhancement 
	1. use 'Output Order Records (pur)' for NYPL and 'Archive Order Records (pur)' for BPL process to compile a list of records into a proper format 
	2. use 'Output MARC Records (out)' process to extract bibs from Sierra and save them locally 
	3. rename files to include library code, example: '220129-NYP.pout' 
	4 keep Sierra's original .pout extension 
4. Drop exported files to the shared drive folder: R:/NSDROP/sierra_dumps/nightshift/ 

### Setup
Using command-line tool initiate NightShift:

```bash
python nightshift/bot.py init local
```

### Usage

The bot and its main process can be launched manually by entring following command in the terminal:

```bash
python nightshift/bot.py run local
```

This process can also be automated using cron/scheduled job on the local system.

On launch Nightshift accesses SFTP/shared drive folder (R:/NSDROP/sierra_dumps/nightshift/) and discovers any new MARC21 files. MARC records are parsed from each file and the bot queries WorldCat database to find suitable matches. 

Next, previously ingested records that have not been successfully enhanced (no suitable match in Worldcat) are queried against Sierra to discover any changes to their status. Upgraded by catalogers or deleted bibs are no longer considered in the following process. If Sierra brief bibs are still in need of enhancement, the bot searches WorldCat for full records again. Processing of older records follow specific to each resource category schedule outlined in `nightshift.constants.RESOURCE_CATEGORIES` dictionary (`query_days`).

If a good match in WorldCat has been found, the bot manipulates the downloaded full record by supplying a call number, deleting specified tags, merging fields from original Sierra bib with WorldCat record, etc. An enhanced in this way record is serialized to MARC21 format and saved to SFTP/shared drive (R:/NSDROP/load/) directory and can be loaded to Sierra. Enhanced records overlay original existing brief bibs in Sierra.

If for any reason the bot is interrupted (API error, etc.), the process can be restarted using `run [local, prod]` command again. The bot should pick up exactly where it left.

## Changelog