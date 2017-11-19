#!/usr/bin/env python
# encoding: utf-8

from pywhdfs.utils import hglob
from threading import Lock
from progressbar import AnimatedMarker, Bar, FileTransferSpeed, Percentage, ProgressBar, RotatingMarker, Timer
import os.path as osp
import os
import sys
import glob

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

    widgets = ['Progress: ', Percentage(), ' ', Bar(left='[',right=']'),
               ' ', Timer(format='Time: %s'), ' ', FileTransferSpeed()]

    if self._total_bytes > 0:
      self.pbar = ProgressBar(widgets=widgets, maxval=self._total_bytes).start()
    else:
      self.pbar = ProgressBar(widgets=widgets, maxval=nfiles).start()

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
      if self._total_bytes:
        self.pbar.update(sum(data.values()))
      else:
        self.pbar.update(self._complete_files)

  def __del__(self):
    if self.pbar:
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
    uploads = [ upload_file for upload_file in glob.iglob(local_path) ]
    if len(uploads) == 0:
      raise HdfsError('Cloud not resolve source path, either it does not exist or can not access it.', local_path)

    nbytes = 0
    nfiles = 0
    for upload in uploads:
      if osp.isdir(upload):
        for dpath, _, fnames in os.walk(upload):
          for fname in fnames:
            nbytes += osp.getsize(osp.join(dpath, fname))
            nfiles += 1
      elif osp.exists(upload):
        nbytes += osp.getsize(upload)
        nfiles += 1
      else:
        raise HdfsError('No file found at: %s', upload)
    return cls(nbytes, nfiles)
