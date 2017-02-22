#!/usr/bin/env python
# encoding: utf-8

from pywhdfs.utils import hglob
from threading import Lock
import sys

class _Progress(object):

  """Progress tracker callback.

  :param nbytes: Total number of bytes that will be transferred.
  :param nfiles: Total number of files that will be transferred.
  :param writer: Writable file-object where the progress will be written.
    Defaults to standard error.

  """

  def __init__(self, nbytes, nfiles, writer=None):
    self._total_bytes = nbytes
    self._pending_files = nfiles
    self._writer = writer or sys.stderr
    self._transferring_files = 0
    self._complete_files = 0
    self._lock = Lock()
    self._data = {}

  def __call__(self, hdfs_path, nbytes):
    # TODO: Improve lock granularity.
    with self._lock:
      data = self._data
      if hdfs_path not in data:
        self._pending_files -= 1
        self._transferring_files += 1
      if nbytes == -1:
        self._transferring_files -= 1
        self._complete_files += 1
      else:
        data[hdfs_path] = nbytes
      if self._pending_files + self._transferring_files > 0:
        self._writer.write(
          '%3.1f%%\t[ pending: %d | transferring: %d | complete: %d ]   \r' %
          (
            100. * sum(data.values()) / self._total_bytes,
            self._pending_files,
            self._transferring_files,
            self._complete_files,
          )
        )
      else:
        self._writer.write('%79s\r' % ('', ))

  @classmethod
  def get_instance(cls, client, hdfs_path, writer=None):
    """Instantiate from remote path.
    :param client: HDFS client.
    :param hdfs_path: HDFS path.
    """

    total_content={'length': 0, 'fileCount': 0}
    matches = [ upload_file for upload_file in hglob.glob(client, hdfs_path) ]
    for file_match in matches:
      file_content = client.content(file_match)
      total_content['length'] +=  file_content['length']
      total_content['fileCount'] +=  file_content['fileCount']
    return cls(total_content['length'], total_content['fileCount'], writer=writer)
