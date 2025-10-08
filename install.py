# installer for the weewx-tigo driver
# Copyright 2025 Matthew Wall
# Distributed under the terms of the GNU Public License (GPLv3)

from weecfg.extension import ExtensionInstaller

def loader():
    return TIGOInstaller()

class TIGOInstaller(ExtensionInstaller):
    def __init__(self):
        super(TIGOInstaller, self).__init__(
            version="0.1",
            name='tigo',
            description='Capture data TIGO solar panel monitor',
            author="Matthew Wall",
            author_email="mwall@users.sourceforge.net",
            files=[('bin/user', ['bin/user/tigo.py'])]
            )
