from calendar import timegm
import dateutil.parser
import json
import logging
import time
import urllib2


def time_diff(start_time, end_time=None):
    """Calculates the difference in seconds between two timestamps"""
    diff = 0.0
    if end_time is None:
        end_time = time.time()
    else:
        end_time = timegm(dateutil.parser.parse(end_time).utctimetuple())
    diff = end_time - timegm(dateutil.parser.parse(start_time).utctimetuple())
    return diff


def str_to_bool(value):
    """Python 2.x does not have a casting mechanism for booleans.  The built in
    bool() will return true for any string with a length greater than 0.  It
    does not cast a string with the text "true" or "false" to the
    corresponding bool value.  This method is a casting function.  It is
    insensetive to case and leading/trailing spaces.  An Exception is raised
    if a cast can not be made.
    """
    value = str(value).strip().lower()
    if value == 'true':
        return True
    elif value == 'false':
        return False
    else:
        raise ValueError('Unable to cast value (%s) to boolean' % value)


def num_healthy_tasks(results):
    """Counts the number of healthy tasks in the marathon environment"""
    count = 0
    if hasattr(results, '__iter__'):
        for i in results:
            if 'alive' in i.keys():
                if i['alive'] is True:
                    count += 1
    else:
        log.error('num_healthy_tasks(): "results" not iterable {0}'
                  .format(results))
    return count


def num_unhealthy_tasks(results):
    """Counts the number of unhealthy tasks in the marathon environment"""
    count = 0
    if hasattr(results, '__iter__'):
        for i in results:
            if 'alive' in i.keys():
                if i['alive'] is False:
                    count += 1
    else:
        log.error('num_unhealthy_tasks(): "results" not iterable {0}'
                  .format(results))
    return count


class Collector:
    """Collector class responsible for collecting information from a host"""
    def __init__(self, host=None, port=None, dimension_paths=None,
                 plugin_instance="unknown"):
        log.debug('Collector.__init__() [{0}:{1}]: invoked'
                  .format(host, port))

        self.host = host
        self.port = port
        self.plugin_instance = plugin_instance

        # Validate the dimension_paths
        if dimension_paths is None:
            self.dimension_paths = {}
        else:
            self.dimension_paths = dimension_paths

        log.debug('Collector.__init__() [{0}:{1}]: complete'
                  .format(self.host, self.port))

    def __repr__(self):
        return str(self.__dict__)

    def _d(self, d):
        """Formats a dictionary of key/value pairs as a comma-delimited list of
        key=value tokens."""
        return ','.join(['='.join(p) for p in d.items()])

    def request(self, version="", api=""):
        """Makes a request to the specified version and api"""
        log.debug('MarathonCollector.request() [{0}:{1}]: invoked'
                  .format(self.host, self.port))
        result = None

        try:
            if version != '':
                request_url = 'http://{host}:{port}/{version}/{api}'.format(
                               host=self.host,
                               port=str(self.port),
                               version=version,
                               api=api)
            else:
                request_url = 'http://{host}:{port}/{api}'.format(
                               host=self.host,
                               port=str(self.port),
                               api=api)
            log.debug('MarathonCollector.request() [{0}:{1}]: Request URL {2}'
                      .format(self.host, self.port, request_url))
            result = json.load(urllib2.urlopen(request_url, timeout=5))
        except urllib2.URLError, e:
            log.error(('MarathonCollector.request() [{0}:{1}]: Error '
                       'connecting to {2}').format(self.host,
                                                   self.port,
                                                   request_url))
        except Exception as e:
            log.error('MarathonCollector.request() [{0}:{1}]: Error {2}'
                      .format(self.host, self.port, e))

        log.debug('MarathonCollector.request() [{0}:{1}]: Response : {2}'
                  .format(self.host,
                          self.port,
                          result))
        log.debug('MarathonCollector.request() [{0}:{1}]: complete'
                  .format(self.host, self.port))
        return result

    def process_dimensions(self, input):
        """Look up a dimensnion in the specified input"""
        log.debug('MarathonCollector.process_dimensions() [{0}:{1}]: invoked'
                  .format(self.host, self.port))
        dimensions = {}
        for dim, path in self.dimension_paths.iteritems():
            value = self.dig_it_up(input, path)
            if value is not None:
                dimensions[dim] = str(value)
            else:
                log.info(('MarathonCollector.process_dimensions() [{0}:{1}]: '
                          'Failed to look up dimension {2}')
                         .format(self.host, self.port, dim))
        log.debug('MarathonCollector.process_dimensions() [{0}:{1}]: complete'
                  .format(self.host, self.port))
        return dimensions

    def dig_it_up(self, obj, path):
        """Find the specified value in the supplied object using the specified
        path"""
        log.debug('MarathonCollector.dig_it_up() [{0}:{1}]: invoked'
                  .format(self.host, self.port))
        result = None
        try:
            if type(path) in (str, unicode):
                path = path.split('/')
            result = reduce(lambda x, y: x[y], path, obj)
        except:
            log.debug(('MarathonCollector.dig_it_up() [{0}:{1}]: failed to dig'
                       ' up {2} in {3}').format(self.host,
                                                self.port,
                                                path,
                                                obj))
        log.debug('MarathonCollector.dig_it_up() [{0}:{1}]: complete'
                  .format(self.host, self.port))
        return result

    def emit(self, name, dimensions, value, metric_type=None, t=None):
        """emit a metric through collectd"""
        log.debug('MarathonCollector.emit() [{0}:{1}]: invoked'
                  .format(self.host, self.port))
        if t is None:
            t = time.time()
        log.info(('Value parameters to be emitted:'
                  '\n name : {n}'
                  '\n dimensions : {d}'
                  '\n value: {v}'
                  '\n type_instance: {ti}'
                  '\n time: {t}').format(n=name,
                                         d=dimensions,
                                         v=str(value),
                                         ti=name,
                                         t=t))
        val = collectd.Values()
        val.plugin = 'marathon'
        val.plugin_instance = self.plugin_instance
        val.plugin_instance += '[{dims}]'.format(dims=self._d(dimensions))
        val.type = metric_type
        val.type_instance = name
        val.time = t
        val.meta = {'true': 'true'}
        val.values = [value]

        log.info('Value to be emitted {0}'.format(str(val)))

        val.dispatch()

        log.debug('MarathonCollector.emit() [{0}:{1}]: complete'
                  .format(self.host, self.port))


class MarathonTaskCollector(Collector):
    def __init__(self, host=None, port=None, version=None,
                 plugin_instance="unknown"):
        log.debug('MarathonTaskCollector.__init__() [{0}:{1}]: invoked'
                  .format(host, port))
        versions = {
            'v2': {
                'api': 'tasks',
                'api_verison': 'v2',
                'dimension_paths': {
                    'mesos_task_id': 'id',
                    'mesos_agent_id': 'slaveId',
                    'mesos_task_name': 'appId'
                },
                'metrics': [
                    {
                        'type': 'gauge',
                        'name': 'marathon.task.start.time.elapsed',
                        'path': 'startedAt',
                        'transformation': time_diff
                     },
                    {
                        'type': 'gauge',
                        'name': 'marathon.task.staged.time.elapsed',
                        'path': 'stagedAt',
                        'transformation': time_diff
                     },
                    {
                        'type': 'gauge',
                        'name': 'marathon.task.healthchecks.passing.total',
                        'path': 'healthCheckResults',
                        'transformation': num_healthy_tasks
                    },
                    {
                        'type': 'gauge',
                        'name': 'marathon.task.healthchecks.failing.total',
                        'path': 'healthCheckResults',
                        'transformation': num_unhealthy_tasks
                    }
                ]
            }
        }
        # Conditionally set the dimensions to collect
        if version in versions.keys():
            Collector.__init__(self,
                               host,
                               port,
                               versions[version]['dimension_paths'],
                               plugin_instance=plugin_instance)
            self.api = versions[version]['api']
            self.metrics = versions[version]['metrics']
        else:
            Collector.__init__(self,
                               host,
                               port,
                               plugin_instance=plugin_instance)
        self.version = version
        log.debug('MarathonTaskCollector.__init__() [{0}:{1}]: complete'
                  .format(self.host, self.port))

    def read(self, marathon_dimensions):
        log.debug('MarathonAppCollector.read() [{0}:{1}]: invoked'
                  .format(self.host, self.port))
        self._tasks = self.get()
        for task in self._tasks:
            start_ts = None
            dimensions = self.process_dimensions(task)
            dimensions.update(marathon_dimensions)

            # Patch mesos_task_name (will need to revisit)
            dimensions['mesos_task_name'] = dimensions['mesos_task_name'][1:]

            for metric in self.metrics:
                name = metric['name']
                metric_type = metric['type']
                value = self.dig_it_up(task, metric['path'])
                if value is not None:
                    if 'transformation' in metric.keys() and name == \
                       'marathon.task.staged.time.elapsed':
                        value = metric['transformation'](value, start_ts)
                    elif 'transformation'in metric.keys() and name == \
                         'marathon.task.start.time.elapsed':
                        start_ts = value
                        value = metric['transformation'](value)
                    elif 'transformation' in metric.keys():
                        value = metric['transformation'](value)
                    self.emit(name, dimensions, value, metric_type)
        log.debug('MarathonAppCollector.read() [{0}:{1}]: complete'
                  .format(self.host, self.port))

    def get(self):
        log.debug('MarathonAppCollector.get() [{0}:{1}]: invoked'
                  .format(self.host, self.port))
        result = []

        try:
            result = self.request(self.version, self.api)['tasks']
        except:
            log.info('MarathonAppCollector.get() [{0}:{1}]: no tasks found'
                     .format(self.host, self.port))

        return result
        log.debug('MarathonAppCollector.get() [{0}:{1}]: complete'
                  .format(self.host, self.port))


class MarathonAppCollector(Collector):
    def __init__(self, host=None, port=None, version=None,
                 plugin_instance="unknown"):
        log.debug('MarathonAppCollector.__init__() [{0}:{1}]: invoked'
                  .format(host, port))
        versions = {
            'v2': {
                'api': 'apps',
                'api_verison': 'v2',
                'dimension_paths': {
                    'mesos_task_name': 'id',  # Equivalent to marathon appId
                    'container_type': 'container/type',
                    'container_image': 'container/docker/image',
                    'container_network': 'container/docker/host'
                },
                'metrics': [
                    {
                        'type': 'gauge',
                        'name': 'marathon.app.cpu.allocated.',
                        'path': 'cpus'
                     },
                    {
                        'type': 'gauge',
                        'name': 'marathon.app.memory.allocated',
                        'path': 'mem'
                     },
                    {
                        'type': 'gauge',
                        'name': 'marathon.app.disk.allocated',
                        'path': 'disk'
                     },
                    {
                        'type': 'gauge',
                        'name': 'marathon.app.gpus.allocated',
                        'path': 'gpus'
                     },
                    {
                        'type': 'gauge',
                        'name': 'marathon.app.tasks.staged',
                        'path': 'tasksStaged'
                     },
                    {
                        'type': 'gauge',
                        'name': 'marathon.app.tasks.running',
                        'path': 'tasksRunning'
                     },
                    {
                        'type': 'gauge',
                        'name': 'marathon.app.tasks.unhealthy',
                        'path': 'tasksUnhealthy'
                     },
                    {
                        'type': 'gauge',
                        'name': 'marathon.app.instances.total',
                        'path': 'instances'
                    },
                    {
                        'type': 'gauge',
                        'name': 'marathon.app.deployments.total',
                        'path': 'deployments',
                        'transformation': lambda x: len(x)
                     }
                ]
            }
        }
        if version in versions.keys():
            Collector.__init__(self,
                               host,
                               port,
                               versions[version]['dimension_paths'],
                               plugin_instance)
            self.api = versions[version]['api']
            self.metrics = versions[version]['metrics']
        else:
            Collector.__init__(self,
                               host,
                               port,
                               plugin_instance=plugin_instance)

        self.version = version
        log.debug('MarathonAppCollector.__init__() [{0}:{1}]: complete'
                  .format(self.host, self.port))

    def read(self, marathon_dimensions):
        self._apps = self.get()
        for app in self._apps:
            dimensions = self.process_dimensions(app)
            dimensions.update(marathon_dimensions)

            # Patch mesos_task_name (will need to revisit)
            dimensions['mesos_task_name'] = dimensions['mesos_task_name'][1:]

            for metric in self.metrics:
                name = metric['name']
                metric_type = metric['type']
                value = self.dig_it_up(app, metric['path'])
                if value is not None:
                    if 'transformation' in metric.keys():
                        value = metric['transformation'](value)
                    self.emit(name, dimensions, value, metric_type)

        # For now collect metrics on queued applications here
        # Should probably move to a separate collector
        self._queue = self.queue()
        for app in self._queue:
            dimensions = self.process_dimensions(app)
            dimensions.update(marathon_dimensions)

            # Patch mesos_task_name (will need to revisit)
            dimensions['mesos_task_name'] = dimensions['mesos_task_name'][1:]

            name = 'marathon.app.delayed'
            metric_type = 'gauge'
            value = self.dig_it_up(app, 'delay/overdue')

            if value is not None:
                value = 0
            else:
                value = 1

            self.emit(name, dimensions, value, metric_type)

    def get(self):
        log.debug('MarathonAppCollector.get() [{0}:{1}]: invoked'
                  .format(self.host, self.port))
        result = []

        try:
            result = self.request(self.version, self.api)['apps']
        except:
            log.info('MarathonAppCollector.get() [{0}:{1}]: no apps found'
                     .format(self.host, self.port))

        return result
        log.debug('MarathonAppCollector.get() [{0}:{1}]: complete'
                  .format(self.host, self.port))

    def queue(self):
        log.debug('MarathonAppCollector.queue() [{0}:{1}]: invoked'
                  .format(self.host, self.port))
        result = []

        try:
            result = self.request(self.version, 'queue')['tasks']
        except:
            log.info('MarathonAppCollector.queue() [{0}:{1}]: empty queue'
                     .format(self.host, self.port))

        log.debug('MarathonAppCollector.queue() [{0}:{1}]: complete'
                  .format(self.host, self.port))
        return result


class MarathonMetricsCollector(Collector):
    def __init__(self, host=None, port=None, version=None):
        log.debug('MarathonMetricsCollector.__init__() [{0}:{1}]: invoked'
                  .format(host, port))
        Collector.__init__(self, host, port)
        self.version = version
        self.api = 'metrics'
        log.debug('MarathonMetricsCollector.__init__() [{0}:{1}]: complete'
                  .format(self.host, self.port))

    def read(self, marathon_dimensions):
        log.debug('MarathonMetricsCollector.read() [{0}:{1}]: invoked'
                  .format(self.host, self.port))
        self._metrics = self.get()
        if hasattr(self._metrics, 'keys'):
            if 'gauges' in self._metrics:
                self.gauges(self._metrics['gauges'], marathon_dimensions)
            if 'counters' in self._metrics:
                self.counters(self._metrics['counters'], marathon_dimensions)
            if 'meters' in self._metrics:
                self.meters(self._metrics['meters'], marathon_dimensions)
        else:
            log.debug(('MarathonMetricsCollector.read() [{0}:{1}]: '
                       'Unable to read results').format(self.host,
                                                        self.port))

        log.debug('MarathonMetricsCollector.read() [{0}:{1}]: complete'
                  .format(self.host, self.port))

    def gauges(self, gauges, dimensions):
        log.debug('MarathonMetricsCollector.gauges() [{0}:{1}]: invoked'
                  .format(self.host, self.port))
        for gauge, info in gauges.iteritems():
            try:
                name = gauge
                metric_type = 'gauge'
                value = info['value']
                self.emit(name, dimensions, value, metric_type)
            except:
                log.info('MarathonMetricsCollector.gauges() [{0}:{1}]: '
                         'No gauges found'.format(self.host, self.port))
        log.debug('MarathonMetricsCollector.gauges() [{0}:{1}]: complete'
                  .format(self.host, self.port))

    def counters(self, counters, dimensions):
        log.debug('MarathonMetricsCollector.counters() [{0}:{1}]: invoked'
                  .format(self.host, self.port))
        for counter, info in counters.iteritems():
            try:
                name = counter
                metric_type = 'counter'
                value = info['count']
                self.emit(name, dimensions, value, metric_type)
            except:
                log.info('MarathonMetricsCollector.counters() [{0}:{1}]: '
                         'No counters found'.format(self.host, self.port))
        log.debug('MarathonMetricsCollector.counters() [{0}:{1}]: complete'
                  .format(self.host, self.port))

    def meters(self, meters, dimensions):
        log.debug('MarathonMetricsCollector.meters() [{0}:{1}]: invoked'
                  .format(self.host, self.port))
        for meter, info in meters.iteritems():
            try:
                name = meter + '.' + info['units'].replace('/', '.per.')
                metric_type = 'gauge'
                value = info['mean_rate']
                self.emit(name, dimensions, value, metric_type)
            except:
                log.info('MarathonMetricsCollector.meters() [{0}:{1}]: '
                         'No meters found'.format(self.host, self.port))
        log.debug('MarathonMetricsCollector.meters() [{0}:{1}]: complete'
                  .format(self.host, self.port))

    def get(self):
        log.debug('MarathonMetricsCollector.get() [{0}:{1}]: invoked'
                  .format(self.host, self.port))
        result = []

        try:
            result = self.request(self.version, self.api)
        except:
            log.info('MarathonMetricsCollector.get() [{0}:{1}]: '
                     'Failed to get metrics'.format(self.host, self.port))

        log.debug('MarathonMetricsCollector.get() [{0}:{1}]: complete'
                  .format(self.host, self.port))
        return result


class MarathonCollector(Collector):
    """The main marathon collector which collects information about a marathon
    host.  Includes collectors for metrics, tasks, and apps.
    """
    def __init__(self, host=None, port=None, version=None):
        versions = {
            'v2': {
                'api': 'info',
                'dimension_paths': {
                    'mesos_framework_id': 'frameworkId',
                    'mesos_framework_name': 'marathon_config/framework_name'
                }
            }
        }
        if version in versions.keys():
            Collector.__init__(self,
                               host,
                               port,
                               versions[version]['dimension_paths'])
            self.api = versions[version]['api']
        else:
            Collector.__init__(self, host, port)

        self.version = version
        # The metrics api endpoint is not versioned
        self.metrics = MarathonMetricsCollector(host, port, '')
        self.tasks = MarathonTaskCollector(host, port, version)
        self.apps = MarathonAppCollector(host, port, version)

    def update_plugin_instance(self, plugin_instance):
        self.plugin_instance = plugin_instance
        self.metrics.plugin_instance = plugin_instance
        self.tasks.plugin_instance = plugin_instance
        self.apps.plugin_instance = plugin_instance

    def read(self):
        log.debug('MarathonCollector.read() [{0}:{1}]: invoked'
                  .format(self.host, self.port))

        # Get general marathon information
        self._info = self.request(self.version, self.api)

        # Parse general dimensions
        self.dimensions = self.process_dimensions(self._info)

        # Update the plugin instances once we know them
        try:
            plugin_instance = '{0}{1}'.format(
                                    self.dimensions['mesos_framework_name'],
                                    self.dimensions['mesos_framework_id'])
            self.update_plugin_instance(plugin_instance)

        except Exception as e:
            log.error(('MarathonCollector.read() [{0}:{1}]: unable to set '
                       'plugin_instance : {2}').format(self.host,
                                                       self.port,
                                                       e))
        self.metrics.read(self.dimensions)
        self.apps.read(self.dimensions)
        self.tasks.read(self.dimensions)
        log.debug('MarathonCollector.read() [{0}:{1}]: complete'
                  .format(self.host, self.port))


class MarathonPlugin:
    """The main plugin class used to configure and orchestrate collectors
    """
    hosts = []

    def __init__(self):
        pass

    def __repr__(self):
        return str(self.__dict__)

    def configure_callback(self, conf):
        log.debug('MarathonPlugin.configure_callback() : invoked')
        for node in conf.children:
            key = str(node.key).lower()
            try:
                if key == 'host':
                    if len(node.values) > 1:
                        host = MarathonCollector(node.values[0],
                                                 node.values[1], 'v2')
                        self.hosts.append(host)
                elif key == 'verbose':
                    handle.verbose = str_to_bool(node.values[0])
            except Exception as e:
                log.error('Failed to load the configuration {0} due to {1}'
                          .format(node.key, e))
        log.info('MarathonPlugin.hosts = %s' % self.hosts)
        log.debug('MarathonPlugin.configure_callback() : compete')

    def init_callback(self):
        log.debug('MarathonPlugin.init_callback() : invoked')
        collectd.register_read(self.read_callback)
        collectd.register_config(self.configure_callback)
        log.debug('MarathonPlugin.init_callback() : complete')
        return True

    def read_callback(self):
        log.debug('MarathonPlugin.read_callback() : invoked')

        # Iterate over host objects
        for host in self.hosts:
            host.read()
        log.debug('MarathonPlugin.read_callback() : complete')

    def flush_callback(self):
        log.debug('MarathonPlugin.flush_callback() : invoked')
        log.debug('MarathonPlugin.flush_callback() : complete')


class CollectdLogHandler(logging.Handler):
    """Log handler to forward statements to collectd
    A custom log handler that forwards log messages raised
    at level debug, info, notice, warning, and error
    to collectd's built in logging.  Suppresses extraneous
    info and debug statements using a "verbose" boolean
    Inherits from logging.Handler
    Arguments
        plugin -- name of the plugin (default 'unknown')
        verbose -- enable/disable verbose messages (default False)
    """
    def __init__(self, plugin="unknown", verbose=False):
        """Initializes CollectdLogHandler
        Arguments
            plugin -- string name of the plugin (default 'unknown')
            verbose -- enable/disable verbose messages (default False)
        """
        self.verbose = verbose
        self.plugin = plugin
        logging.Handler.__init__(self, level=logging.NOTSET)

    def emit(self, record):
        """
        Emits a log record to the appropraite collectd log function
        Arguments
        record -- str log record to be emitted
        """
        try:
            if record.msg is not None:
                if record.levelname == 'ERROR':
                    collectd.error('%s : %s' % (self.plugin, record.msg))
                elif record.levelname == 'WARNING':
                    collectd.warning('%s : %s' % (self.plugin, record.msg))
                elif record.levelname == 'NOTICE':
                    collectd.notice('%s : %s' % (self.plugin, record.msg))
                elif record.levelname == 'INFO' and self.verbose is True:
                    collectd.info('%s : %s' % (self.plugin, record.msg))
                elif record.levelname == 'DEBUG' and self.verbose is True:
                    collectd.debug('%s : %s' % (self.plugin, record.msg))
        except Exception as e:
            collectd.warning(('{p} [ERROR]: Failed to write log statement due '
                              'to: {e}').format(p=self.plugin,
                                                e=e
                                                ))


class CollectdLogger(logging.Logger):
    """Logs all collectd log levels via python's logging library
    Custom python logger that forwards log statements at
    level: debug, info, notice, warning, error
    Inherits from logging.Logger
    Arguments
    name -- name of the logger
    level -- log level to filter by
    """
    def __init__(self, name, level=logging.NOTSET):
        """Initializes CollectdLogger
        Arguments
        name -- name of the logger
        level -- log level to filter by
        """
        logging.Logger.__init__(self, name, level)
        logging.addLevelName(25, 'NOTICE')

    def notice(self, msg):
        """Logs a 'NOTICE' level statement at level 25
        Arguments
        msg - log statement to be logged as 'NOTICE'
        """
        self.log(25, msg)


# Set up logging
logging.setLoggerClass(CollectdLogger)
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.propagate = False
handle = CollectdLogHandler('Marathon')
log.addHandler(handle)

if __name__ == '__main__':
    import os
    import sys

    class ExecCollectdValues:
        def dispatch(self):
            if not getattr(self, 'host', None):
                self.host = os.environ.get('COLLECTD_HOSTNAME', 'localhost')
            identifier = '%s/%s' % (self.host, self.plugin)
            if getattr(self, 'plugin_instance', None):
                identifier += '-' + self.plugin_instance
            identifier += '/' + self.type
            if getattr(self, 'type_instance', None):
                identifier += '-' + self.type_instance
            print 'PUTVAL', identifier, \
                  ':'.join(map(str, [int(self.time)] + self.values))

    class ExecCollectd:
        def Values(self):
            return ExecCollectdValues()

        def register_read(self, callback):
            pass

        def register_config(self, callback):
            pass

        def register_init(self, callback):
            pass

        def register_flush(self, callback):
            pass

        def error(self, msg):
            print 'ERROR: ', msg

        def warning(self, msg):
            print 'WARNING:', msg

        def notice(self, msg):
            print 'NOTICE: ', msg

        def info(self, msg):
            print 'INFO:', msg

        def debug(self, msg):
            print 'DEBUG: ', msg

    class MockCollectdConfigurations():
        def __init__(self):
            self.children = []

    class MockConfiguration():
        def __init__(self, key, values):
            self.key = key
            self.values = values

    # Instantiate required objects
    handle.verbose = True
    collectd = ExecCollectd()
    plugin = MarathonPlugin()
    configs = MockCollectdConfigurations()

    # Configurations
    mock_configurations = [
        {'host': ['192.168.65.111', '2797']},
        {'verbose': ['true']}
    ]

    # Mock collectd configurations
    for conf in mock_configurations:
        for k, v in conf.iteritems():
            configs.children.append(MockConfiguration(k, v))

    # Send mock configurations to configuration callback
    plugin.configure_callback(configs)

    if len(sys.argv) > 1:
        plugin.marathon_url = sys.argv[1]

    if not plugin.init_callback():
        sys.exit(1)

    while True:
        plugin.read_callback()
        time.sleep(1)


else:
    import collectd

    plugin = MarathonPlugin()

    collectd.register_config(plugin.configure_callback)
    collectd.register_init(plugin.init_callback)
