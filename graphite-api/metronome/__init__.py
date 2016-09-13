import json
import time
import re
import logging
from threading import Lock

from graphite_api.intervals import Interval, IntervalSet
from graphite_api.node import LeafNode, BranchNode, Node

import requests


URLLENGTH = 8000
DEFAULT_METRICS_CACHE_EXPIRY = 300

log = logging.getLogger(__name__)


def chunk(nodelist, length):
    """Splits lists of nodes so that they fit within url limits"""
    chunklist = []
    linelength = 0
    for node in nodelist:
        # the magic number 1 is because the nodes list is joined with ','
        nodelength = len(str(node)) + 1

        if linelength + nodelength > length:
            yield chunklist
            chunklist = [node]
            linelength = nodelength
        else:
            chunklist.append(node)
            linelength += nodelength
    yield chunklist


def load_jsonp(s):
    """Metronome always returns JSONP with invalid JSON inside"""
    #log.debug('JSONP data |%s|', s)
    # Strip '_(' and ');'
    raw = s[2:-2]
    # Fix broken JSON
    raw = raw.replace(' derivative: ', ' "derivative":') \
             .replace(' raw: ', ' "raw":')
    return json.loads(raw)


class Matcher(object):
    """Tests candidate paths against 'foo.*.{a,b}.latency' query expressions"""

    def __init__(self, query):
        self.query = query

        self.regexp = (
            # Group captures result
            '^(?P<path>' +
            query.replace('.', r'\.').replace('$', r'\$')
                 # '{a,b}' -> /(a|b)/
                 .replace('{', '(').replace('}', ')').replace(',', '|')
                 # '*' -> /[^.]*/
                 .replace('*', r'[^.]*')
            # Group captures extra path info if not a leaf node
            + ')(?P<extra>$|\..+$)'
        )
        self.re = re.compile(self.regexp)
        #log.debug('Matcher regexp: %s', self.regexp)

    def match(self, candidate_path):
        # Returns: (path, is_leaf_node)
        m = self.re.match(candidate_path)
        if m:
            is_leaf_node = not m.group('extra')
            return m.group('path'), is_leaf_node
        else:
            return None, None


class MetronomeFinder(object):
    """Main entrypoint for the plugin"""

    __fetch_multi__ = 'metronome'

    def __init__(self, config=None):
        self._metronome_url = config['metronome']['url']
        self._metrics_cache_expiry = \
            config['metronome'].get('metrics_cache_expiry',
                                    DEFAULT_METRICS_CACHE_EXPIRY)
        self._metrics_lock = Lock()
        log.info('MetronomeFinder initialized: %s', self._metronome_url)

    def find_nodes(self, query):
        """Find nodes for 'foo.*.{a,b}.latency' query expressions
        :type query: graphite_api.storage.FindQuery
        """
        with self._metrics_lock:
            metrics = self._get_metrics_list()

        matcher = Matcher(query.pattern)
        seen = set()

        for candidate in metrics:
            path, is_leaf_node = matcher.match(candidate)
            if not path:
                continue

            if path in seen:
                continue
            seen.add(path)

            #log.debug('match: %s %s', path, is_leaf_node)

            if is_leaf_node:
                yield MetronomeLeafNode(path, MetronomeReader(path, self))
            else:
                yield BranchNode(path)

    _metrics_cache = None
    _metrics_cache_ts = 0
    def _get_metrics_list(self):
        """Get raw list of all metrics from Metronome"""
        if self._metrics_cache_ts + self._metrics_cache_expiry > time.time():
            return self._metrics_cache

        resp = requests.get(self._metronome_url,
                            params=dict(do='get-metrics', callback='_'))
        data = load_jsonp(resp.text)

        self._metrics_cache = data['metrics']
        self._metrics_cache_ts = time.time()
        log.info('Loaded %i metric paths', len(self._metrics_cache))
        return self._metrics_cache

    def fetch_multi(self, nodes, start_time, end_time):
        """Fetch data for multiple nodes"""
        paths = [ node.path for node in nodes ]
        log.info('fetch_multi: %s', ' '.join(paths))

        data = {}
        # FIXME: can we get the requested number?
        points = min(2000, (end_time - start_time) / 10)
        start_time = start_time
        end_time = end_time
        step = (end_time - start_time) / points

        # The chunking splits it into multiple requests if we would exceed
        # the maximum url path length
        for pathlist in chunk(paths, URLLENGTH):
            series = self._retrieve_data(pathlist, start_time, end_time, points)
            data.update(series)
            """
            tmpdata = requests.get(urls.metrics,
                                   params={'path': pathlist,
                                           'from': start_time,
                                           'to': end_time}).json()
            if 'error' in tmpdata:
                return (start_time, end_time, end_time - start_time), {}

            if 'series' in data:
                data['series'].update(tmpdata['series'])
            else:
                data = tmpdata
            """

        time_info = start_time, end_time, step
        return time_info, data

    def _retrieve_data(self, paths, start_time, end_time, points):
        log.debug('_retrieve_data %s [%s %s %s]',
                  ' '.join(paths), start_time, end_time, points)
        params = dict(
            do='retrieve',
            name=','.join(paths),
            begin=start_time,
            end=end_time,
            datapoints=points,
            callback='_'
        )
        resp = requests.get(self._metronome_url, params=params)
        data = load_jsonp(resp.text)
        return {
            path: [ val for (ts, val) in series ]
            for path, series in data['raw'].items()
        }


class MetronomeLeafNode(LeafNode):
    __fetch_multi__ = 'metronome'


class MetronomeReader(object):
    """Reads data for a single path"""

    __slots__ = ('path', '_finder')

    def __init__(self, path, finder):
        """
        :type path: str
        :type finder: MetronomeFinder
        """
        self.path = path
        self._finder = finder

    def fetch(self, start_time, end_time):
        time_info, series = \
            self._finder.fetch_multi([Node(self.path)], start_time, end_time)

        if self.path in series:
            single_series = series[self.path]
            #if not single_series:
            #    return None
            return time_info, single_series
        else:
            return time_info, []

    def get_intervals(self):
        # TODO: can we return real data?
        return IntervalSet([Interval(0, int(time.time()))])

