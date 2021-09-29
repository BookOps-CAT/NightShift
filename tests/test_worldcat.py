# -*- coding: utf-8 -*-

import pytest


from nightshift import __title__, __version__
from nightshift.worldcat import get_credentials


@pytest.mark.parametrize("arg", ["NYP", "BPL"])
def test_get_credentials(arg, mock_worldcat_creds):
    assert (get_credentials(library=arg)) == {
        "key": "lib_key",
        "secret": "lib_secret",
        "scopes": "WorldCatMetadataAPI",
        "principal_id": "lib_principal_id",
        "principal_idns": "lib_principal_idns",
        "agent": f"{__title__}/{__version__}",
    }
