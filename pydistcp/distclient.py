#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import os
import sys
import glob
import re
import logging as lg
import os.path as osp
from pywhdfs.client import WebHDFSClient
from pywhdfs.utils import hglob
from pywhdfs.utils.utils import HdfsError
from multiprocessing.pool import ThreadPool
from threading import Lock
from datetime import datetime

_logger = lg.getLogger(__name__)

class WebHDFSDistClient(object):

  """HDFS web client using Hadoop token delegation security.
  :param url: Hostname or IP address of HDFS namenode, prefixed with protocol,
    followed by WebHDFS port on namenode
  :param token: Hadoop delegation token.
  :param \*\*kwargs: Keyword arguments passed to the base class' constructor.
  """

  def __init__(self, src, dst, **kwargs):
  	self.src=src
  	self.dst=dst
  	_logger.info('Instantiated %r.', self)

  def __repr__(self):
    return '<%s(urls=%r),%s(urls=%r)>' % (self.src.__class__.__name__, self.src.host_list, self.dst.__class__.__name__, self.dst.host_list)

  def copy(self, src_path, dst_path, overwrite=False, n_threads=1, preserve=False,
    chunk_size=2 ** 16, checksum=True, progress=None, **kwargs):
    """Copy a file or directory to HDFS.

    :param dst_path: Target HDFS path. If it already exists and is a
      directory, files will be copied inside.
    :param src_path: Source HDFS path to copy. If a folder, all the files
      inside of it will be copied (note that this implies that folders empty
      of files will not be created ), when pattern is used this will act
      as a root path.
    :param overwrite: Overwrite any existing file or directory.
    :param n_threads: Number of threads to use for parallelization. A value of
      `0` (or negative) uses as many threads as there are files.
    :param chunk_size: Interval in bytes by which the files will be copied.
    :param progress: Callback function to track progress, called every
      `chunk_size` bytes. It will be passed two arguments, the path to the
      file being copied and the number of bytes transferred so far. On
      completion, it will be called once with `-1` as second argument.
    :param \*\*kwargs: Keyword arguments forwarded to :meth:`write`.

    On success, this method returns the remote copyed path.

    """
    start_time = time.time()
    if not chunk_size:
      raise ValueError('Copy chunk size must be positive.')

    lock = Lock()
    stat_lock = Lock()

    _logger.info('Copying %r to %r.', src_path, dst_path)

    def _preserve(_src_path, _dst_path):
      # set the base path attributes

      srcstats = self.src.status(_src_path)
      _logger.debug("Preserving %r source attributes on %r" % (_src_path,_dst_path))
      self.dst.set_owner(_dst_path, owner=srcstats['owner'], group=srcstats['group'])
      self.dst.set_permission(_dst_path, permission=srcstats['permission'])
      self.dst.set_times(_dst_path, access_time=srcstats['accessTime'], modification_time=srcstats['modificationTime'])
      if srcstats['type'] == 'FILE':
        self.dst.set_replication(_dst_path, replication=int(srcstats['replication']))

    def _copy_wrap(_path_tuple):
      _src_path, _dst_path = _path_tuple
      try:
        status = _copy(_path_tuple)
        return status
      except Exception as exp:
        _logger.exception('Error while copying %r to %r. %s' % (_src_path,_dst_path,exp))
        return { 'status': 'failed', 'src_path': _src_path, 'dest_path' : _dst_path }

    def _copy(_path_tuple):
      """Copy a single file."""

      def wrap(_reader, _chunk_size, _progress):
        """Generator that can track progress."""
        nbytes = 0
        while True:
          #print "read here %s from %s" % (_chunk_size, _src_path)
          chunk = _reader.read(_chunk_size)
          if chunk:
            if _progress:
              nbytes += len(chunk)
              _progress(_src_path, nbytes)
            yield chunk
          else:
            break
        if _progress:
          _progress(_src_path, -1)

      _src_path, _dst_path = _path_tuple
      _tmp_path = ""

      skip=False

      dst_st=self.dst.status(_dst_path,strict=False)

      if dst_st == None:
        # destination does not exist
        _tmp_path=_dst_path
      else:
        # destination exist
        if not overwrite:
          raise HdfsError('Destination file exist and Missing overwrite parameter.')
        _tmp_path = '%s.temp-%s' % (_dst_path, int(time.time()))

        if checksum == True:
          _src_path_checksum = self.src.checksum(_src_path)
          _dst_path_checksum = self.dst.checksum(_dst_path)
          if _src_path_checksum['algorithm'] != _dst_path_checksum['algorithm']:
            _logger.debug('source and destination files does not seems to have the same block size or crc chunk size.')
          elif _src_path_checksum['bytes'] != _dst_path_checksum['bytes']:
            _logger.debug('source destination files does not seems to have the same checksum value.')
          else:
            _logger.debug('source %r and destination %r seems to be identical, skipping.', _src_path, _dst_path)
            skip=True
        else:
          _logger.debug('no checksum check will be performed, forcing file copy source %r to destination %r.', _src_path, _dst_path)
          #skip=True

      if not skip:
        # Prevent race condition when creating directories
        with lock:
          if self.dst.status(osp.dirname(_tmp_path), strict=False) is None:
            _logger.debug('Parent directory %r does not exist, creating recursively.', osp.dirname(_tmp_path))
            curpath = ''
            root_dir = None
            for dirname in osp.dirname(_tmp_path).strip('/').split('/'):
              curpath = '/'.join([curpath, dirname])
              if self.dst.status(curpath, strict=False) is None:
                if root_dir is not None:
                  root_dir = curpath
                self.dst.makedirs(curpath)
                if preserve:
                  curr_src_path=osp.realpath( osp.join( _src_path,osp.relpath(curpath,_tmp_path)) )
                  _preserve(curr_src_path,curpath)
      
        _logger.debug('Copying %r to %r.', _src_path, _tmp_path)

        if preserve:
          srcstats = self.src.status(_src_path)
          kwargs['replication'] = int(srcstats['replication'])
          kwargs['blocksize'] = int(srcstats['blockSize'])

        with self.src.read(_src_path) as _reader:
          self.dst.write(_tmp_path, wrap(_reader, chunk_size, progress), **kwargs)

        if _tmp_path != _dst_path:
          _logger.debug( 'Copy of %r complete. Moving from %r to %r.', _src_path, _tmp_path, _dst_path )
          self.dst.delete(_dst_path)
          self.dst.rename(_tmp_path, _dst_path)
        else:
          _logger.debug(
            'Copy of %r to %r complete.', _src_path, _dst_path
          )
      
        if preserve:
          _preserve(_src_path,_dst_path)

        return { 'status': 'copied', 'src_path': _src_path, 'dest_path' : _dst_path }
      else:
        # file was skipped
        if progress:
          src_st=self.src.status(_src_path,strict=False)
          progress(_src_path, int(src_st['length']))
          progress(_src_path, -1)
        return { 'status': 'skipped', 'src_path': _src_path, 'dest_path' : _dst_path }

    # Normalise src and dst paths
    src_path = self.src.resolvepath(src_path)
    dst_path = self.dst.resolvepath(dst_path)

    # First, resolve the list of src files/directories to be copied
    copies = [ copy_file for copy_file in hglob.glob(self.src, src_path) ]
 
    # need to develop a propper pattern based access function
    if len(copies) == 0:
      raise HdfsError('Cloud not resolve source path %s, either it does not exist or can not access it.', src_path)

    tuples = []
    for copy in copies:
      copy_tuple = dict()
      try:
        #filename = osp.basename(copy)
        #dst_base_path =  osp.join( dst_path, filename )
        status = self.dst.status(dst_path,strict=True)
        #statuses = [status for _, status in self.dst.list(dst_base_path)]
      except HdfsError as err:
        if 'File does not exist' in str(err):
          # Remote path doesn't exist.
          # check if parent exist
          try:
            pstatus = self.dst.status(osp.dirname(dst_path),strict=True)
          except HdfsError, err:
            raise HdfsError('Parent directory of %r does not exist.', dst_path)
          else:
            # Remote path does not exist, and parent exist
            # so we want the source to be renamed as destination
            # so do not add the basename
            dst_base_path =  dst_path
            copy_tuple = dict({ 'src_path' : copy, 'dst_path' : dst_base_path})
      else:
        # Remote path exists.
        if status['type'] == 'FILE':
          # Remote path exists and is a normal file.
          if not overwrite:
            raise HdfsError('Destination path %r already exists.', dst_path)
          # the file is going to be deleted and the destination is going to be created with the same name
          dst_base_path = dst_path
        else:
          # Remote path exists and is a directory.
          try:
            status = self.dst.status(osp.join( dst_path, osp.basename(copy) ),strict=True)
          except HdfsError as err:
            if 'File does not exist' in str(err):
              # destination does not exist, great !
              dst_base_path =  osp.join( dst_path, osp.basename(copy) )
              pass
          else:
            # destination exists
            dst_base_path = osp.join( dst_path, osp.basename(copy))
            if not overwrite:
              raise HdfsError('Destination path %r already exists.', dst_base_path)

        copy_tuple = dict({ 'src_path' : copy, 'dst_path' : dst_base_path})
      finally:
        tuples.append(copy_tuple)

    # This is a workaround for a Bug when copying files using a pattern
    # it may happen that files can have the same name:
    # ex : /home/user/test/*/*.py may result in duplicate files
    for i in range(0, len(tuples)):
        for x in range(i + 1, len(tuples)):
          if tuples[i]['dst_path'] == tuples[x]['dst_path']:
            raise HdfsError('Conflicting files %r and %r : can\'t copy both files to %r'
                            % (tuples[i]['src_path'], tuples[x]['src_path'], tuples[i]['dst_path']) )

    fpath_tuples = []
    for copy_tuple in tuples:
      # Then we figure out which files we need to copy, and where.
      src_paths = list(self.src.walk(copy_tuple['src_path']))
      if not src_paths:
        # This is a single file.
        src_fpaths = [copy_tuple['src_path']]
      else:
        src_fpaths = [
          osp.join(dpath, fname)
          for dpath, _, fnames in src_paths
          for fname in fnames
        ]

      offset = len(copy_tuple['src_path'].rstrip(os.sep)) + len(os.sep)

      fpath_tuples.extend([
          (
            fpath,
            osp.join(copy_tuple['dst_path'], fpath[offset:].replace(os.sep, '/')).rstrip(os.sep)
          )
          for fpath in src_fpaths
      ])

    # Finally, we copy all files (optionally, in parallel).
    if n_threads <= 0:
      n_threads = len(fpath_tuples)
    else:
      n_threads = min(n_threads, len(fpath_tuples))
    _logger.debug(
      'Copying %s files using %s thread(s).', len(fpath_tuples), n_threads
    )

    try:
      if n_threads == 1:
        results = []
        for path_tuple in fpath_tuples:
          results.append( _copy_wrap(path_tuple) )
      else:
        results = _map_async(n_threads, _copy_wrap, fpath_tuples)
    except Exception as err: # pylint: disable=broad-except
      _logger.exception('Error while copying.')
      raise err

    end_time = time.time()

    # Transfer summary
    status = {
      'Source Path'      : src_path,
      'Destination Path' : dst_path,
      'Start Time'       : datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S'),
      'End Time'         : datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S'),
      'Duration'         : end_time - start_time,
      'Outcome'          : 'Successful',
      'Files Expected'   : 0,
      'Size Expected'    : 0,
      'Files Copied'     : 0,
      'Size Copied'      : 0,
      'Files Failed'     : 0,
      'Size Failed'      : 0,
      'Files Deleted'    : 0,
      'Size Deleted'     : 0,
      'Files Skipped'    : 0,
      'Size Skipped'     : 0,
    }

    for result in results:
      file_st = self.src.status(result['src_path'],strict=True)
      status['Files Expected']+=1
      status['Size Expected']+=int(file_st['length'])
      if result['status'] == 'copied':
        status['Files Copied']+=1
        status['Size Copied']+=int(file_st['length'])
      if result['status'] == 'skipped':
        status['Files Skipped']+=1
        status['Size Skipped']+=int(file_st['length'])
      if result['status'] == 'failed':
        status['Files Failed']+=1
        status['Size Failed']+=int(file_st['length'])
        status['Outcome'] = 'Failed'

    return status

# Helpers
# -------

def _map_async(pool_size, func, args):
  """Async map (threading), handling python 2.6 edge case.

  :param pool_size: Maximum number of threads.
  :param func: Function to run.
  :param args: Iterable of arguments (one per thread).
  """
  pool = ThreadPool(pool_size)
  if sys.version_info <= (2, 6):
    return pool.map(func, args)
  else:
    return pool.map_async(func, args).get(1 << 31)

