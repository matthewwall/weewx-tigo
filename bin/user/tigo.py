#!/usr/bin/env python
# Copyright 2025 Matthew Wall
# Distributed under the terms of the GNU Public License (GPLv3)
"""
Collect data from TIGO solar panel monitor over RS485 using taptap.
"""

from __future__ import with_statement
import datetime
import os
import re
import subprocess
import threading
import time

try:
    # Python 3
    import queue
except ImportError:
    # Python 2:
    import Queue as queue

try:
    import cjson as json
    setattr(json, 'dumps', json.encode)
    setattr(json, 'loads', json.decode)
except (ImportError, AttributeError):
    try:
        import simplejson as json
    except ImportError:
        import json

import weewx.drivers
import weewx.units

try:
    # logging in weewx 4+
    import weeutil.logger
    import logging
    log = logging.getLogger(__name__)
    def logdbg(msg):
        log.debug(msg)
    def loginf(msg):
        log.info(msg)
    def logerr(msg):
        log.error(msg)
except ImportError:
    # logging in weewx 3
    import syslog
    def logmsg(level, msg):
        syslog.syslog(level, 'tigo: %s: %s' %
                      (threading.currentThread().getName(), msg))
    def logdbg(msg):
        logmsg(syslog.LOG_DEBUG, msg)
    def loginf(msg):
        logmsg(syslog.LOG_INFO, msg)
    def logerr(msg):
        logmsg(syslog.LOG_ERR, msg)

# FIXME: gotta figure out how to make logging work for direct invocation
#def logdbg(msg):
#    print(msg)
#def loginf(msg):
#    print(msg)
#def logerr(msg):
#    print(msg)

DRIVER_NAME = 'TIGO'
DRIVER_VERSION = '0.1'

def loader(config_dict, _):
    return TIGODriver(**config_dict[DRIVER_NAME])

def confeditor_loader():
    return TIGOConfigurationEditor()

# these are fields that we report in each observation cycle.
OBS_FIELDS = [
    'voltage_in',
    'voltage_out',
    'current',
    'dc_dc_duty_cycle',
    'temperature',
    'rssi',
]

MAX_PANELS = 50
schema = [('dateTime', 'INTEGER NOT NULL UNIQUE PRIMARY KEY'),
          ('usUnits', 'INTEGER NOT NULL'),
          ('interval', 'INTEGER NOT NULL')] + \
[('p%s_voltage_in' % (i + 1), 'REAL') for i in range(MAX_PANELS)] + \
[('p%s_voltage_out' % (i + 1), 'REAL') for i in range(MAX_PANELS)] + \
[('p%s_current' % (i + 1), 'REAL') for i in range(MAX_PANELS)] + \
[('p%s_dc_dc_duty_cycle' % (i + 1), 'REAL') for i in range(MAX_PANELS)] + \
[('p%s_temperature' % (i + 1), 'REAL') for i in range(MAX_PANELS)] + \
[('p%s_rssi' % (i + 1), 'REAL') for i in range(MAX_PANELS)]

weewx.units.obs_group_dict['voltage_in'] = 'group_volt'
weewx.units.obs_group_dict['voltage_out'] = 'group_volt'
weewx.units.obs_group_dict['current'] = 'group_amp'
weewx.units.obs_group_dict['temperature'] = 'group_temperature'

TS = re.compile('(\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d\d\d\d\d\d)\d\d\d(.*)')
def to_datetime(s):
    # taptap returns times in this format:
    #   2025-10-08T12:38:05.081063171-04:00
    # convert that to unix epoch in UTC
    # unfortunately datetime handles only usec, not nsec, so we have to strip
    # some digits from the time string.
    m = TS.search(s)
    if m:
        s = "%s%s" % (m.group(1), m.group(2))
    ts = datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()
    return ts


class AsyncReader(threading.Thread):

    def __init__(self, fd, queue, label):
        threading.Thread.__init__(self)
        self._fd = fd
        self._queue = queue
        self._running = False
        self.daemon = True
        self.name = label

    def run(self):
        logdbg("start async reader for %s" % self.name)
        self._running = True
        for line in iter(self._fd.readline, ''):
            if line:
                self._queue.put(line)
            if not self._running:
                break

    def stop_running(self):
        self._running = False


class ProcManager(object):

    def __init__(self):
        self._cmd = None
        self._process = None
        self.stdout_queue = queue.Queue()
        self.stdout_reader = None
        self.stderr_queue = queue.Queue()
        self.stderr_reader = None

    def startup(self, cmd, path=None, ld_library_path=None):
        self._cmd = cmd
        loginf("startup process '%s'" % self._cmd)
        env = os.environ.copy()
        if path:
            env['PATH'] = path + ':' + env['PATH']
        if ld_library_path:
            env['LD_LIBRARY_PATH'] = ld_library_path
        try:
            self._process = subprocess.Popen(cmd.split(' '),
                                             env=env,
                                             stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE)
            self.stdout_reader = AsyncReader(
                self._process.stdout, self.stdout_queue, 'stdout-thread')
            self.stdout_reader.start()
            self.stderr_reader = AsyncReader(
                self._process.stderr, self.stderr_queue, 'stderr-thread')
            self.stderr_reader.start()
        except (OSError, ValueError) as e:
            raise weewx.WeeWxIOError("failed to start process '%s': %s" %
                                     (cmd, e))

    def shutdown(self):
        loginf('shutdown process %s' % self._cmd)
        self._process.kill()
        logdbg("close stdout")
        self._process.stdout.close()
        logdbg("close stderr")
        self._process.stderr.close()
        logdbg('shutdown %s' % self.stdout_reader.getName())
        self.stdout_reader.stop_running()
        self.stdout_reader.join(0.5)
        logdbg('shutdown %s' % self.stderr_reader.getName())
        self.stderr_reader.stop_running()
        self.stderr_reader.join(0.5)
        if self._process.poll() is None:
            logerr('process did not respond to kill, shutting down anyway')
        self._process = None
        if self.stdout_reader.is_alive():
            loginf('timed out waiting for %s' % self.stdout_reader.getName())
        self.stdout_reader = None
        if self.stderr_reader.is_alive():
            loginf('timed out waiting for %s' % self.stderr_reader.getName())
        self.stderr_reader = None
        loginf('shutdown complete')

    def running(self):
        return self._process.poll() is None

    def get_stderr(self):
        lines = []
        while not self.stderr_queue.empty():
            lines.append(self.stderr_queue.get().decode())
        return lines

    def get_stdout(self):
        lines = []
        while self.running():
            try:
                line = self.stdout_queue.get(True, 3)
                yield line
            except queue.Empty:
                pass


class TIGOConfigurationEditor(weewx.drivers.AbstractConfEditor):
    @property
    def default_stanza(self):
        return """
[TIGO]
    # The driver to use
    driver = user.tigo
    # The name of the tap device, e.g., /dev/ttyUSB0 or hostname.lan:7160
    tap = REPLACE_ME

    [[panel_map]]
        # This section maps the broker_id.node_id pairs to the panel indices
        # that form the database field names.
        p1 = 0000.01
        p2 = 0000.02
"""
    def prompt_for_settings(self):
        print("Specify the name of the tap device")
        tap = self._prompt('tap', '/dev/ttyUSB0')
        return {'tap', tap}


class TIGODriver(weewx.drivers.AbstractDevice):

    def __init__(self, **stn_dict):
        loginf('driver version is %s' % DRIVER_VERSION)
        tap = stn_dict.get('tap', '/dev/ttyUSB0')
        loginf('tap=%s' % tap)
        path = stn_dict.get('path', None)
        ld_library_path = stn_dict.get('ld_library_path', None)
        app = stn_dict.get('app', 'taptap')
        if tap.startswith('/'):
            devstr = '--serial %s' % tap
        else:
            parts = tap.split(':')
            devstr = '--tcp %s' % parts[0]
            if len(parts) == 2:
                devstr += ' --port %s' % parts[1]
        cmd = "%s observe %s" % (app, devstr)
        loginf("cmd='%s'" % cmd)
        self._panel_map = stn_dict.get('panel_map', {})
        loginf("panel_map=%s" % self._panel_map)
        self._known_identifiers = []
        self._mgr = ProcManager()
        self._mgr.startup(cmd, path, ld_library_path)

    def closePort(self):
        self._mgr.shutdown()

    @property
    def hardware_name(self):
        return "TIGO"

    def genLoopPackets(self):
        while self._mgr.running():
            for line in self._mgr.get_stdout():
                obj = self.parse_json(line)
                if obj:
                    pkt = self.map_packet(obj)
                    if pkt:
                        yield pkt
            # report any errors
            for line in self._mgr.get_stderr():
                logerr(line)
        else:
            for line in self._mgr.get_stderr():
                logerr(line)
            raise weewx.WeeWxIOError("taptap process is not running")

    def parse_json(self, line):
        try:
            return json.loads(line)
        except ValueError as e:
            logdbg("parse_json failed: %s" % e)
        return None

    def map_packet(self, obj):
        # map an identified object to a channel
        pkt = dict()
        identifier = '%s.%s' % (obj['gateway']['id'], obj['node']['id'])
        label = self.get_panel_label(identifier)
        if label is None:
            if identifier not in self._known_identifiers:
                loginf("no panel mapping found for identifier '%s'" % identifier)
                self._known_identifiers.append(identifier)
            label = identifier
        pkt['dateTime'] = to_datetime(obj['timestamp'])
        pkt['usUnits'] = weewx.METRIC
        pkt['identifier'] = identifier
        for field in OBS_FIELDS:
            if field in obj:
                pkt['%s_%s' % (label, field)] = float(obj[field])
        return pkt

    def get_panel_label(self, identifier):
        # given an identifier of the form gateway_id.node_id, return the
        # associated panel label.
        for label in self._panel_map:
            if self._panel_map[label] == identifier:
                return label
        return None


def get_panel_map(config_filename):
    from weecfg import read_config
    panel_map = dict()
    config_path, config_dict = read_config(config_filename)
    if 'TIGO' in config_dict and 'panel_map' in config_dict['TIGO']:
        panel_map = config_dict['TIGO']['panel_map']
    return panel_map

    
def main():
    import optparse
    from weeutil.weeutil import to_sorted_string

    usage = """%prog [--debug] [--help] [--version]
        [--path=PATH] [--ld_library_path=LD_LIBRARY_PATH]
    """

    parser = optparse.OptionParser(usage=usage)
    parser.add_option('--version', action='store_true',
                      help='display driver version')
    parser.add_option('--debug', action='store_true',
                      help='display diagnostic information while running')
    parser.add_option('--app', default='taptap',
                      help='the taptap command')
    parser.add_option('--path',
                      help='value for PATH')
    parser.add_option('--ld_library_path',
                      help='value for LD_LIBRARY_PATH')
    parser.add_option('--tap', default='/dev/ttyUSB0',
                      help='name of the tap device or host[:port]')
    parser.add_option('--config',
                      help='configuration file with channel map')
    parser.add_option('--action', default='show-data',
                      help='what to do: show-data, list-identifiers')

    (options, args) = parser.parse_args()

    if options.version:
        print("tigo driver version %s" % DRIVER_VERSION)
        exit(0)

    if options.debug:
        weewx.debug = 1
    if options.action not in ['show-data', 'list-identifiers']:
        print("unknown action '%s'" % options.action)
        exit(1)

    panel_map = dict()
    if options.config:
        panel_map = get_panel_map(options.config)

    config_dict = {
        'TIGO': {
            'tap': options.tap,
            'app': options.app,
            'panel_map': panel_map,
        }
    }
    if options.path:
        config_dict['TIGO']['path'] = options.path
    if options.ld_library_path:
        config_dict['TIGO']['ld_library_path'] = options.ld_library_path

    driver = loader(config_dict, None)

    if options.action == 'show-data':
        for pkt in driver.genLoopPackets():
            print(to_sorted_string(pkt))
    elif options.action == 'list-identifiers':
        duration = 30 # how long to listen, in seconds
        print("listening for %s seconds" % duration)
        ts = time.time()
        detected_identifiers = []
        for pkt in driver.genLoopPackets():
            identifier = pkt['identifier']
            if identifier not in detected_identifiers:
                detected_identifiers.append(identifier)
            if time.time() - ts > duration:
                break
        print("found %s unique identifiers" % len(detected_identifiers))
        for identifier in sorted(detected_identifiers):
            print("  %s" % identifier)
        if options.config:
            # if a configuration file was specified, get the panel mapping from
            # it (if one exists) then use that to see whether any of the
            # identifiers that we have detected is not mapped.
            known_identifiers = []
            panel_map = get_panel_map(options.config)
            for label in panel_map:
                known_identifiers.append(panel_map[label])
            unrecognized_identifiers = []
            for identifier in detected_identifiers:
                if identifier not in known_identifiers:
                    unrecognized_identifiers.append(identifier)
            if unrecognized_identifiers:
                print("unrecognized:")
                for identifier in sorted(unrecognized_identifiers):
                    print("  %s" % identifier)

if __name__ == '__main__':
    main()
