import functools
import sys
import json
import time
import re
import logging
import threading
from multiprocessing.pool import ThreadPool

from graphite_api.intervals import Interval, IntervalSet
from graphite_api.node import LeafNode, BranchNode, Node

import requests


# yahttp (used in Metronome) has a default max url length of 2048 bytes
# We account for other paramaters here. The 300 is empirical, and seems to have
# to include all headers sent by the client, otherwise you end up with
# 404 responses...
URLLENGTH = 2048 - 300

# Default cache timeout for the list of metric paths
DEFAULT_METRICS_CACHE_EXPIRY = 300

log = logging.getLogger(__name__)

pool = ThreadPool(processes=4)
request_session_cache = threading.local()


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

    if chunklist:
        yield chunklist


def load_jsonp(s):
    """Metronome always returns JSONP with invalid JSON inside"""
    #log.debug('JSONP data |%s|', s)
    # Strip '_(' and ');'
    raw = s[2:-2]
    # Fix broken JSON
    raw = raw.replace(' derivative: ', ' "derivative":') \
             .replace(' raw: ', ' "raw":')

    try:
        data = json.loads(raw)
    except ValueError:
        log.error('Invalid JSONP:\n%s', s)
        raise
    return data


def log_call(func):
    """Decorator to log calls to functions"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        t0 = time.time()
        result = func(*args, **kwargs)
        t1 = time.time()
        log.debug('Call to %s took %.3fs', func.func_name, t1 - t0)
        return result
    return wrapper


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


class _LastFetch:
    """Cached last fetch data to handle movingAverage"""
    start_time = 0
    end_time = 0
    step = 0
    points = 0

    additional_points = 0
    ext_start_time = 0
    ext_points = 0
    ext_data = None


class MetronomeFinder(object):
    """Main entrypoint for the plugin"""

    __fetch_multi__ = 'metronome'

    def __init__(self, config=None):
        self._metronome_url = config['metronome']['url']
        self._metrics_cache_expiry = \
            config['metronome'].get('metrics_cache_expiry',
                                    DEFAULT_METRICS_CACHE_EXPIRY)

        # Cache the last data fetch, because the graphite-api first does a
        # fetch_multi and single fetches for movingAverage.
        self._last_fetch = _LastFetch()

        log.info('MetronomeFinder initialized: %s', self._metronome_url)

    def find_nodes(self, query):
        """Find nodes for 'foo.*.{a,b}.latency' query expressions
        :type query: graphite_api.storage.FindQuery
        """
        metrics, metrics_set = self._get_metrics_list()

        # Shortcut if there is no wildcard
        if not '{' in query.pattern and not '*' in query.pattern:
            path = query.pattern
            if path in metrics_set:
                yield MetronomeLeafNode(path, MetronomeReader(path, self))
            return

        log.info("find_nodes: %s", query.pattern)
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
    _metrics_cache_set = None
    _metrics_cache_ts = 0
    def _get_metrics_list(self):
        """Get raw list of all metrics from Metronome"""
        if self._metrics_cache_ts + self._metrics_cache_expiry > time.time():
            return self._metrics_cache, self._metrics_cache_set

        resp = requests.get(self._metronome_url,
                            params=dict(do='get-metrics', callback='_'))
        data = load_jsonp(resp.text)
        log.info('Loaded %i metric paths', len(data['metrics']))

        # Extend available metrics with mapped view names
        self._metrics_cache = self._pdns_map_views(data['metrics'])
        self._metrics_cache_set = set(self._metrics_cache)
        self._metrics_cache_ts = time.time()

        return self._metrics_cache, self._metrics_cache_set

    _r_pdns_map_views = re.compile(
        r'^pdns\.(?P<name>.+)\.(?P<type>auth|recursor)\.(?P<extra>.+?)$'
    )
    def _pdns_map_views(self, paths):
        """Add virtual view metrics that reorganize Metronome PDNS data

        Fixes metric paths that not in a proper format, and makes it easy to
        just select all recursors in Grafana.

            pdns.foo.auth.* -> _pdns_view.auth.foo.auth.*
            pdns.foo.recursor.* -> _pdns_view.recursor.foo.recursor.*
            pdns.a.example.com.auth.* -> _pdns_view.auth.a--example--com.auth.*

        This way you can use `_pdns_view.recursor.*` as a Grafana template
        query.
        """
        view_paths = []
        for path in paths:
            view_paths.append(path)
            m = self._r_pdns_map_views.match(path)
            if m:
                new_name = m.group('name').replace('.', '--')
                view = '_pdns_view.{type}.{name}.{type}.{extra}'.format(
                    type=m.group('type'),
                    name=new_name,
                    extra=m.group('extra'))
                view_paths.append(view)

        # Add time derivative version of metric (always returned by metronome)
        with_dt = []
        for path in view_paths:
            with_dt.append(path)
            with_dt.append(path + '_dt')
        return with_dt

    def _pdns_unmap_views(self, paths):
        """Reverse view mapping before fetching data"""
        unmapped = []
        renames = {}
        for path in paths:
            if path.startswith('_pdns_view.'):
                p = path.split('.')
                new_path = 'pdns.{name}.{type}.{extra}'.format(
                    name=p[2].replace('--', '.'),
                    type=p[1],
                    extra='.'.join(p[4:])
                )
                renames[new_path] = path
                unmapped.append(new_path)
            else:
                unmapped.append(path)
        return unmapped, renames

    @log_call
    def fetch_multi(self, nodes, start_time, end_time):
        """Fetch data for multiple nodes"""
        nodes_str = ','.join( x.path for x in nodes[:3] )
        if len(nodes) > 3:
            nodes_str += '...+{}'.format(len(nodes) - 3)
        log.info('fetch_multi: %s [%s,%s>',
                  nodes_str, start_time, end_time)

        if len(nodes) == 1:
            # Return cached data if this request is triggered by movingAverage()
            path = nodes[0].path
            time_info, values = \
                self._fetch_from_last(path, start_time, end_time)
            if time_info is not None:
                return time_info, {path: values}

        # Rename view paths to real metric paths for querying
        paths, renames = self._pdns_unmap_views([ node.path for node in nodes ])

        # Might use this hack to get the maximum number of points in the future
        #caller = sys._getframe(2)
        #request = caller.f_locals.get('request_options')
        #log.warn("@@@@ %s", request)

        # FIXME: can we get the requested number?
        points = min(720, (end_time - start_time) / 10)
        start_time = start_time
        end_time = end_time
        step = (end_time - start_time) / points
        time_info = start_time, end_time, step

        # Request extra data for movingAverage() that will be cached
        # and requested later
        additional_points = 100 # TODO: pick a good value
        ext_points = points + additional_points
        ext_start_time = start_time - additional_points * step

        # The chunking splits it into multiple requests if we would exceed
        # the maximum url path length
        # TODO: not entirely correct anymore with the path manipulation, but
        #       still works
        def do_retrieve(pathlist):
            try:
                return self._retrieve_data(
                    pathlist, ext_start_time, end_time, ext_points)
            except Exception as e:
                log.exception('Exception in do_retrieve')
                raise

        # These are executed in parallel with a thread pool
        ext_data = {}
        for series in pool.map(do_retrieve, chunk(paths, URLLENGTH)):
            ext_data.update(series)

        # Restore view names for the result
        for old, new in renames.items():
            ext_data[new] = ext_data.pop(old)

        # Cache the last data fetch, because the graphite-api first does a
        # fetch_multi and then repeats it one by one.
        last = self._last_fetch
        last.start_time = start_time
        last.end_time = end_time
        last.step = step
        last.points = points
        last.additional_points = additional_points
        last.ext_start_time = ext_start_time
        last.ext_points = ext_points
        last.ext_data = ext_data

        # Now strip the extended points for the result the caller requested
        data = {
            path: values[additional_points:]
            for path, values in ext_data.items()
        }

        return time_info, data

    def _fetch_from_last(self, path, start_time, end_time):
        # Return cached data if this is a request triggered by movingAverage(),
        # which can be identified by an end_time equal to last start_time
        last = self._last_fetch
        if last.ext_data is None:
            return None, None

        end_time_match = end_time == last.start_time
        has_range = start_time >= last.ext_start_time
        has_path = path in last.ext_data
        #log.debug(
        #    '_fetch_from_last: cache (%s): '
        #        'end_time_match=%s has_range=%s has_path=%s',
        #    path, end_time_match, has_range, has_path)

        if end_time_match and has_range and has_path:
            points = int((end_time - start_time) / last.step)
            from_point = last.additional_points - points
            to_point = last.additional_points
            values = last.ext_data[path][from_point:to_point]
            time_info = start_time, end_time, last.step

            log.debug(
                '_fetch_from_last: movingAverage cached data: '
                    '[%s,%s> out of [%s~%s,%s> for %s (%i points)',
                start_time, end_time,
                last.ext_start_time, last.start_time,
                last.end_time,
                path, points)

            return time_info, values

        return None, None

    def _retrieve_data(self, paths, start_time, end_time, points):
        # The _dt indicated the derivative version of the data
        base_paths = []
        for path in paths:
            if path.endswith('_dt'):
                path = path[:-3]
            if not path in base_paths:
                base_paths.append(path)

        log.debug('_retrieve_data %s [n=%i start=%s end=%s points=%s]',
            ' '.join(base_paths), len(base_paths),
            start_time, end_time, points)

        t0 = time.time()
        params = dict(
            do='retrieve',
            name=','.join(base_paths),
            begin=start_time,
            end=end_time,
            datapoints=points,
            callback='_'
        )

        # Allows for keepalive
        #session = getattr(request_session_cache, 'session', None)
        #if session is None:
        #    session = requests.session()
        #    request_session_cache.session = session
        session = requests.session()

        try:
            resp = session.get(self._metronome_url, params=params)
        except requests.RequestException as e:
            log.error("Exception while fetching data: %s", str(e))
            raise

        if resp.status_code != 200:
            log.error(
                '_retrieve_data: response code %s != 200', resp.status_code)
            return {}

        data = load_jsonp(resp.text)
        t1 = time.time()
        size_kb = len(resp.text) / 1024.0
        kbps = size_kb / (t1 - t0)
        log.debug('_retrieve_data took %.1fs for %i paths (%.1f kB; %.1f kB/s)',
                  t1 - t0, len(base_paths), size_kb, kbps)

        series_dict = {}
        for path, series in data['raw'].items():
            if path in paths:
                series_dict[path] = [ val for (ts, val) in series ]
        for path, series in data['derivative'].items():
            if path + '_dt' in paths:
                series_dict[path + '_dt'] = [ val for (ts, val) in series ]

        return series_dict


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

    # noinspection PyProtectedMember
    @log_call
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

