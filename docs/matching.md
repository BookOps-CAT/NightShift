# WorldCat record matching

NightShift uses WorldCat Metadata API /brief-bibs endpoint to query for matching records. Query strategy depends on searched resource type and available data on to-be-upgraded record in Sierra.

A successful match in Worldcat does not quarantee that the record will be accepted for enrichment. Additionally, after a full MARC XML is obtained, the bot performs assement if the match meets certain minimum criteria:
+ no all upper case title
+ record has a statement of responsibility in 245$c
+ record has physical description in the 300 tag
+ record has at least one subject tag (6xx)
+ call number for the material type can be successfuly created

## e-resources (OverDrive MARC Express bibs)
+ ebooks:
    + the bot uses Reserve ID encoded in the 037$a field of MARC Express records as a primary matchpoint for records in WorldCat: "q" - query parameter for fielded search (example, `/brief-bibs?q=sn=257ECC79-E421-4785-B08A-FCA084579F73`)
    + excludes encoding level 3 records (`lv:3`)
    + excludes records originating from certain institutions (see `nightshift.constants.ROTTEN_APPLES` for details)
    + limits results to English cataloging language (`inCatalogLanguage=eng`)
    + limits to item type to "book" (`itemType=book`)
    + limits to item sub type "book-digital" (`itemSubType=book-digital`)
    + order results to most popular (`orderBy=mostWidelyHeld`)
+ e-audio:
    + the bot uses Reserve ID encoded in the 037$a field of MARC Express records as a primary matchpoint for records in WorldCat: "q" - query parameter for fielded search (example, `/brief-bibs?q=sn=257ECC79-E421-4785-B08A-FCA084579F73`)
    + excludes encoding level 3 records (`lv:3`)
    + excludes records originating from certain institutions (see `nightshift.constants.ROTTEN_APPLES` for details)
    + limits results to English cataloging language (`inCatalogLanguage=eng`)
    + limits to item type to "book" (`itemType=audiobook`)
    + limits to item sub type "book-digital" (`itemSubType=audiobook-digital`)
    + order results to most popular (`orderBy=mostWidelyHeld`)
+ e-video:
    + the bot uses Reserve ID encoded in the 037$a field of MARC Express records as a primary matchpoint for records in WorldCat: "q" - query parameter for fielded search (example, `/brief-bibs?q=sn=257ECC79-E421-4785-B08A-FCA084579F73`)
    + excludes encoding level 3 records (`lv:3`)
    + excludes records originating from certain institutions (see `nightshift.constants.ROTTEN_APPLES` for details)
    + limits results to English cataloging language (`inCatalogLanguage=eng`)
    + limits to item type to "book" (`itemType=video`)
    + limits to item sub type "book-digital" (`itemSubType=video-digital`)
    + order results to most popular (`orderBy=mostWidelyHeld`)
+ print monograph:
    !!NOT FINAL!!
    + the bot uses ISBN then LCCN to find matches (`q=bn:9780137043293` then `q=ln:2012010892`)
    + limits to records created by Library of Congress (`catalogSource=dlc`)
    + limits results to English cataloging language (`inCatalogLanguage=eng`)
    + limits to item type to "book" (`itemType=book`)
    + limits to item sub type "book-digital" (`itemSubType=book-printbook`)
    + order results to most popular (`orderBy=mostWidelyHeld`)
