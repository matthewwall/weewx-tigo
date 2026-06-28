# installer for the weewx-tigo driver
# Copyright 2025 Matthew Wall
# Distributed under the terms of the GNU Public License (GPLv3)

from io import StringIO
import configobj
from weecfg.extension import ExtensionInstaller

CFG_DEFAULTS = u"""
[TIGO]
    driver = user.tigo

    # The RS485 tap is either a serial device or hostname:port
    tap = /dev/ttyUSB0

[DataBindings]
    [[tigo_binding]]
        database = tigo_sqlite
        table_name = archive
        manager = weewx.manager.DaySummaryManager
        schema = user.tigo.schema

[Databases]
    [[tigo_sqlite]]
        database_name = tigo.sdb
        database_type = SQLite
"""

defaults_dict = configobj.ConfigObj(StringIO(CFG_DEFAULTS), encoding='utf-8')

def loader():
    return TIGOInstaller()

class TIGOInstaller(ExtensionInstaller):
    def __init__(self):
        super(TIGOInstaller, self).__init__(
            version="0.2",
            name='tigo',
            description='Capture data from Tigo solar panel optimizers',
            author="Matthew Wall",
            author_email="mwall@users.sourceforge.net",
            config=defaults_dict,
            files=[('bin/user', ['bin/user/tigo.py'])]
            )

    def configure(self, engine):
        engine.config_dict['Station']['station_type'] = 'TIGO'
        engine.config_dict['StdArchive']['data_binding'] = 'tigo_binding'
