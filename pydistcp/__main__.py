#!/usr/bin/env python
# encoding: utf-8

"""pydistcp: A python Web HDFS based tool for inter/intra-cluster data copying.

Usage:
  pydistcp [-fp] [--silent] (-s CLUSTER -d CLUSTER) [-v...] [-t THREADS] SRC_PATH DEST_PATH
  pydistcp (-V | -h)

Options:
  -V --version                  Show version and exit.
  -h --help                     Show help and exit.
  -s CLUSTER --src=CLUSTER      Alias of source namenode to connect to (valid only with dist).
  -d CLUSTER --dest=CLUSTER     Alias of destination namenode to connect to (valid only with dist).
  -v --verbose                  Enable log output. Can be specified up to three
                                times (increasing verbosity each time).
  --silent                      Don't display progress status.
  -f --force                    Allow overwriting any existing files.
  -p --preserve                 Preserve file attributes.
  -t THREADS --threads=THREADS  Number of threads to use for parallelization.
                                0 allocates a thread per file. [default: 0]

Examples:
  pydistcp -s prod -d preprod -v /tmp/src /tmp/dest

"""

from . import __version__
from pywhdfs.config import WebHDFSConfig
from pywhdfs.utils.utils import *
from docopt import docopt
from .distclient import WebHDFSDistClient
from .utils import _Progress
import logging as lg
import requests as rq
import json
import sys


def parse_arg(args, name, parser, separator=None):
  """Parse command line argument, raising an appropriate error on failure.

  :param args: Arguments dictionary.
  :param name: Name of option to look up.
  :param parser: Function to parse option.
  :param separator: For parsing lists.

  """
  value = args[name]
  if not value:
    return
  try:
    if separator and separator in value:
      return [parser(part) for part in value.split(separator) if part]
    else:
      return parser(value)
  except ValueError:
    raise HdfsError('Invalid %r option: %r.', name, args[name])

def configure_client(args):
  """Instantiate configuration from arguments dictionary.

  :param args: Arguments returned by `docopt`.
  :param config: CLI configuration, used for testing.

  If the `--log` argument is set, this method will print active file handler
  paths and exit the process.

  """
  # capture warnings issued by the warnings module  
  try:
    # This is not available in python 2.6
    lg.captureWarnings(True)
  except:
    # disable annoying url3lib warnings
    rq.packages.urllib3.disable_warnings()
    pass

  logger = lg.getLogger()
  logger.setLevel(lg.DEBUG)
  # lg.getLogger('requests_kerberos.kerberos_').setLevel(lg.INFO)
  # logger.addFilter(AnnoyingErrorsFilter())

  levels = {0: lg.CRITICAL, 1: lg.ERROR, 2: lg.WARNING, 3: lg.INFO}

  # Configure stream logging if applicable
  stream_handler = lg.StreamHandler()
  # This defaults to zero
  stream_log_level=levels.get(args['--verbose'], lg.DEBUG)
  stream_handler.setLevel(stream_log_level)

  fmt = '%(levelname)s\t%(message)s'
  stream_handler.setFormatter(lg.Formatter(fmt))
  logger.addHandler(stream_handler)

  config = WebHDFSConfig()

  # configure file logging if applicable
  handler = config.get_log_handler()
  logger.addHandler(handler)

  src_client = config.get_client(args["--src"])
  dest_client = config.get_client(args["--dest"])
  return WebHDFSDistClient(src_client, dest_client)

def main(argv=None):
  """Entry point.

  :param argv: Arguments list.
  :param client: For testing.

  """
  args = docopt(__doc__, argv=argv, version=__version__)

  client = configure_client(args)

  n_threads = parse_arg(args, '--threads', int)
  force = args['--force']
  silent = args['--silent']  
  src_path = args['SRC_PATH']
  dest_path = args['DEST_PATH']

  if sys.stderr.isatty() and not silent:
    progress = _Progress.from_hdfs_pattern(client.src,src_path)
  else:
    progress = None

  status = client.copy(
            src_path,
            dest_path,
            overwrite=force,
            n_threads=n_threads,
            progress=progress,
            preserve= True if args['--preserve'] else False,
          )

  print "Job Status:"
  print json.dumps(status, indent=2)
  sys.exit(0)
  
if __name__ == '__main__':
  main()
