#!/usr/bin/env python
# encoding: utf-8

import abc
import hashlib
import time
import cherrypy
from cherrypy.lib import static

class JobsStack:
     def __init__(self):
         self.items = []

     def isEmpty(self):
         return self.items == []

     def push(self, item):
         self.items.append(item)

     def pop(self):
         return self.items.pop()

     def peek(self):
         return self.items[len(self.items)-1] if len(self.items) > 0 else None

     def size(self):
         return len(self.items)

class Job(threading.Thread):

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        threading.Thread.__init__(self, group=group, target=target, name=name,
                                  verbose=verbose)
        self.args = args
        self.kwargs = kwargs
        
		self.state = "SUBMITTED"
		self.jobID = "job-%s-%s" % ( hash_object = hashlib.md5(job_specs.hexdigest()), time.strftime("%d%m%Y%H%M%S") )


    @abc.abstractmethod
    def get_job_schema(self):
        """Returns a Json schema string describing the job"""
        return json.loads(resource_string(__name__, 'resources/<job-type>_schema.json'))

    def validate(self):
        try:
          self.schema = get_job_schema()
          #self.schema = open("resources/schema.config").read()
          try:
            js.validate(self.job_specs, self.schema)
          except js.ValidationError as e:
            print e.message
          except js.SchemaError as e:
            print e

    @abc.abstractmethod
    def initialize(self):
        """Initialize the Job"""
        return 

    @abc.abstractmethod
    def run(self):
        """Run the Job"""
        return 

class UploadJob(Job):

	def __init__(self, job_specs):
		self.job_specs = job_specs

	def run(self):
	  self.state = "RUNNING"

	  n_threads = int(self.job_specs['n_threads'])
      part_size = int(self.job_specs['part_size'])
      buffer_size = int(self.job_specs['buffer_size'])
      include_pattern = self.job_specs['include_pattern']
      min_size = int(self.job_specs['min_size'])
      force = self.job_specs['force']
      checksum = False if self.job_specs['checksum'] else True
      files_only = True if self.job_specs['files_only'] else False
      src_endpoint = self.job_specs['src_endpoint']
      src_path = self.job_specs['src_path']
      dest_path = self.job_specs['dest_path']

	  config = WebHDFSConfig()
	  client = config.get_client(src_endpoint)
	  status = client.upload(
                dst_path,
                src_path,
                overwrite=overwrite,
                checksum=checksum,
                chunk_size=chunk_size,
                n_threads=n_threads,
                include_pattern=include_pattern,
                files_only=files_only,
                min_size=min_size,
                preserve= preserve,
              )
	  self.state = "FINISHED"

class CopyJob(Job):

	def __init__(self, job_specs):
		self.job_specs = job_specs

	def run(self):
	  self.state = "RUNNING"

	  n_threads = int(self.job_specs['n_threads'])
      part_size = int(self.job_specs['part_size'])
      buffer_size = int(self.job_specs['buffer_size'])
      include_pattern = self.job_specs['include_pattern']
      min_size = int(self.job_specs['min_size'])
      force = self.job_specs['force']
      checksum = False if self.job_specs['checksum'] else True
      files_only = True if self.job_specs['files_only'] else False
      src_endpoint = self.job_specs['src_endpoint']
      dst_endpoint = self.job_specs['dst_endpoint']
      src_path = self.job_specs['src_path']
      dest_path = self.job_specs['dest_path']

	  config = WebHDFSConfig()
	  src_client = config.get_client(src_endpoint)
      dest_client = config.get_client(dst_endpoint)
      client = WebHDFSDistClient(src_client, dest_client)

	  status = client.copy(
              src_path,
              dest_path,
              overwrite=overwrite,
              checksum=checksum,
              chunk_size=chunk_size,
              buffer_size=buffer_size,
              n_threads=n_threads,
              preserve=preserve,
            )
	  self.state = "FINISHED"

class JobScheduler(object):

	_logger = lg.getLogger(__name__)

	def __init__(self):
		self.jobs = JobsStack()

	def submit_job(self,job):
		self.jobs.push(job)

    def job_scheduler(self):
    	while True:
    		peek = self.jobs.peek()
    		if peek != None:
    			if peek.state == "SUBMITTED":
    				peek.start()

class DistMaster(object):

	_logger = lg.getLogger(__name__)
    
    def __init__(self):
    	# Create the the job stack
    	self.jobs = JobsStack()

    @cherrypy.expose
    def get_job_status(self, job_id):
    	return None

    @cherrypy.expose
    def cancel_job(self, job_id):
    	return None

	@cherrypy.expose
	@cherrypy.tools.json_out()
    @cherrypy.tools.json_in()
    def submit_job(self):
    	job_specs = cherrypy.request.json
        if (job_specs['type'] == 'copy'):
        	job = CopyJob(job_specs['job'])
        elif (job_specs['type'] == 'upload'):
        	job = UploadJob(job_specs['job'])
        else:
        	return {"Error": "missing or unsupported job type.", "result": "failed"}
