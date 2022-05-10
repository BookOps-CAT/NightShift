![tests](https://github.com/BookOps-CAT/NightShift/actions/workflows/tests.yaml/badge.svg?branch=main) [![Coverage Status](https://coveralls.io/repos/github/BookOps-CAT/NightShift/badge.svg?branch=main)](https://coveralls.io/github/BookOps-CAT/NightShift?branch=main) [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# NightShift
NightShift is a copy cataloging bot. It processes exported from ILS/Sierra bibliographic and order data, searches WorldCat for full bib matches, and produces enriched this way MARC21 records that are used to replace initial vendor or brief bibs in the catalog.


 [![Overview](https://github.com/BookOps-CAT/NightShift/blob/main/docs/media/NightShift-concept-0.1.0-2022-04-12.png)](https://github.com/BookOps-CAT/NightShift/blob/main/docs/media/NightShift-concept-0.1.0-2022-04-12.png)


At the moment, NightShift is capable of enhancement of OverDrive MarcExpress records only. The app utilizes [WorldCat Metadata API](https://www.oclc.org/developer/api/oclc-apis/worldcat-metadata-api.en.html) to query and obtain full MARC XML records from WorldCat. Enriched records are output to a SFTP/network drive from where they can be accessed to be [loaded back to Sierra](https://github.com/BookOps-CAT/NightShift/blob/documentation/docs/load.md). 

## Version
> 0.1.0

## Local Installation & Usage

### Prerequisites
1. PostgresSQL
	1. download and install [PostgreSQL server](https://www.postgresql.org/download/) 
	2. create new database 
2. Create a configuration file 
	Follow `nightshift/config/config.yaml.example` to provide all required credentials
		+ [WorldCat Metadata API](https://www.oclc.org/developer/api/oclc-apis/worldcat-metadata-api.en.html)credentials for BPL and NYPL
		+ [NYPL Platform](https://platformdocs.nypl.org/) credentials
		+ BPL Solr credentials (request from BPL Web Applications)
		+ NYPL loggly token (NYPL ITG)
		+ SFTP credentials (NYPL ITG)
3. Export Sierra brief bibs for enhancement 
	1. use 'Output Order Records (pur)' for NYPL and 'Archive Order Records (pur)' for BPL process to compile a list of records into a proper format 
	2. use 'Output MARC Records (out)' process to extract bibs from Sierra and save them locally 
	3. rename files to include library code ("BPL" or "NYP") and 'pout' suffix, example: 'NYP-MarcExpress-pout-220129.out'
4. Drop exported files to the shared drive folder: R:/NSDROP/sierra_dumps/nightshift/

### Setup
Using command-line tool initiate NightShift:

```bash
python nightshift/bot.py init local
```

### Usage

The bot and its main process can be launched manually by entering following command in the terminal:

```bash
python nightshift/bot.py run local
```

This process can also be automated using cron/scheduled job on the local system.

On launch Nightshift accesses SFTP/shared drive folder (R:/NSDROP/sierra_dumps/nightshift/) and discovers any new MARC21 files. MARC records are parsed from each file and the bot queries WorldCat database to find suitable matches. 

Next, previously ingested records that have not been successfully enhanced (no suitable match in WorldCat) are queried against Sierra to discover any changes to their status. Upgraded by catalogers or deleted bibs are no longer considered in the following process. If Sierra brief bibs are still in need of enhancement, the bot searches WorldCat for full records again. Processing of older records follow specific to each resource category schedule outlined in `nightshift.constants.RESOURCE_CATEGORIES` dictionary (`query_days`).

If a good match in WorldCat has been found, the bot manipulates the downloaded full record by supplying a call number, deleting specified tags, merging fields from original Sierra bib with WorldCat record, etc. An enhanced in this way record is serialized to MARC21 format and saved to SFTP/shared drive (R:/NSDROP/load/) directory and can be loaded to Sierra. Enhanced records overlay original existing brief bibs in Sierra.

If for any reason the execution of the routine is interrupted (API error, etc.), the process can be restarted using `run [local, prod]` command again. The bot will pick up exactly where it left.

## Changelog