# -*- coding: utf-8 -*-

"""
This module provides methods to generate MARC records

MARC manipulation rules:
Use OCLC matching record as a base

    shared:
        - remove 020 from OCLC full bib
        - remove 024 from OCLc full bib
        - remove 037s from OCLC full bib
        - remove local fields 019, 084, 938
        - remove subject heading tags different than LCSH, LCGN, GSAFD, FAST
        - remove 856s tags from full OCLC bib

        - add 020 based on MarcExpress bib (ME)
        - add 024 based on ME bib
        - add 037$a based on ME bib

    NYPL only:
        - 001  includes OCLC number without a prefix
        - create 091  $a eBOOK/eAudio/eVideo based on Sierra format value
        - create 949 command line to set Sierra format, target bib
        - add NYPL specific 856s based on ME 

    BPL only:
        - 001  includes OCLC number with a prefix
        - create 099  $a eBook/eAudio/eVideo based on Sierra format value
        - create 949 command line to set Sierra format, target bib
        - add NYPL specific 856s based on ME 

"""
