# NightShift record manipulation

The bot during parsing of Sierra bibs for enrichment stores some of the MARC fields to use them instead of their couterparts on the full WordCat record. These fields are defined in `nightshift.constants.RESOURCE_CATEGORIES` dictionary as `srcTags2Keep`. This dictionary also defines MARC tags of the Worldcat record that should be removed from the record (`dstTags2Delete`).

When a full record is obtained from WorldCat, Nighshfit performs following manipulations:
1. Deletes any tags specified in the `nightshift.constants.RESOURCE_CATEGORIES` `dstTags2Delete` (most common are 020, 029, 037, 090, 263, 856, and 9xx)
2. Removes MARC fields that include terms from unsupported thesauri (only LCSH, FAST, GSAFD, LCGFD, HOMOIT are accepted)
3. Removes any 710 tags that include e-resource vendor names (OverDrive, CloudLibrary, 3M, Recorded Books)
4. Determines if the record meets minimum criteria:
    + call number can be created based on supported material format and content
    + does not have ALL upper case title (245 MARC tag)
    + has a statement of responsibility (245$c)
    + has physical description (300 MARC tag)
    + has at least one 6xx tags with a term from any of the supported subject vocabularies (LCSH, FAST, LCGFT, GSAFD, HOMOIT)
5. If needed adds any required genre tags (655):
    + ebooks: `655  \0$a Electronic books.`
    + e-audiobooks: `655  \7$a Audiobooks. $2 lcgft` & `655  \7$a Electronic audiobooks. $2 local`
    + e-video: `655  \7$a Internet videos. $2 lcgft`
6. Adds preserved `srcTags2Keep` to the record
7. Adds Sierra bib # for re-import matching purposes:
    + BPL: `907  \\$a.b12345678a`
    + NYPL: `945  \\$a.b12345678a`
8.  Adds import command tag (949 tag) specifying particular Sierra bib format code (`b2`), suppression status (`b3`), location (`bn`), example: `949  \\$a*b2=x;bn:ia;`
9. Adds initials tag:
    + BPL: `901  \\$aNightShift/{version}`
    + NYPL: `947  \\$aNightShift/{version}`
10. For NYPL records removes OCLC prefix from the 001 control field.

