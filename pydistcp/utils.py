#!/usr/bin/env python
# encoding: utf-8

from pywhdfs.utils import hglob
from threading import Lock
from progressbar import AnimatedMarker, Bar, FileTransferSpeed, Percentage, ProgressBar, RotatingMarker, Timer
import os.path as osp
import os
import sys

class _Progress(object):

  """Progress tracker callback.

  :param nbytes: Total number of bytes that will be transferred.
  :param nfiles: Total number of files that will be transferred.
    Defaults to standard error.

  """

  def __init__(self, nbytes, nfiles):
    self._total_bytes = nbytes
    self._pending_files = nfiles
    self._transferring_files = 0
    self._complete_files = 0
    self._lock = Lock()
    self._data = {}

    widgets = ['Progress: ', Percentage(), ' ', Bar(marker=RotatingMarker()),
               ' ', Timer(), ' ', FileTransferSpeed()]

    self.pbar = ProgressBar(widgets=widgets, maxval=self._total_bytes).start()

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
      self.pbar.update(sum(data.values()))

  def __del__(self):
    self.pbar.finish()

  @classmethod
  def from_hdfs(cls, client, hdfs_path):
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
    return cls(total_content['length'], total_content['fileCount'])

  @classmethod
  def from_local(cls, local_path):
    """Instantiate from a local path.
    :param local_path: Local path.
    """
    if osp.isdir(local_path):
      nbytes = 0
      nfiles = 0
      for dpath, _, fnames in os.walk(local_path):
        for fname in fnames:
          nbytes += osp.getsize(osp.join(dpath, fname))
          nfiles += 1
    elif osp.exists(local_path):
      nbytes = osp.getsize(local_path)
      nfiles = 1
    else:
      raise HdfsError('No file found at: %s', local_path)
    return cls(nbytes, nfiles)