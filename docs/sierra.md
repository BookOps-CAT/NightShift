# Sierra Scheduler configuration

To automate extraction of bibliographic and order data from ILS, Sierra Scheduler can be used.
The Scheduler is capable of running daily predefine SQL queries (lists) and outputting them to a SFTP server.

Following query should be defined to run daily list of OverDrive Marc Express records:
`BIBLIOGRAPHIC CREATED equals yesterday AND BIBLIOGRAPHIC BIB UTIL # starts with  "odn"`

Next, the Scheduler should output records on the created list using `pur` process (Archive order records) and transfer resulting file to the SFTP.

The exported file should have following naming convention: 
[3-chr uppercase library code]-nightshift-pout.YYYYMMDDHHSS

Example:
+ `NYP-nightshift-pout.20220131065301`
+ `BPL-nightshift-pout.20220131065301`

The date as an extension is a required element of the Scheduler functionality and does not need to be configured.