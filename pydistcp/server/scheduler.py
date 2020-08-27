from datetime import datetime
from threading import Timer
from . import now
from . import start_web_interface

port = 8765


class Scheduler(object):
	"""
	Manages Job objects and the waiting between job executions
	"""
	def __init__(self, log=None):
		self.jobs = []
		self.current_job = None
		self.sleeper = None
		self.running = False
		if log:
			self.log = log
		else:
			import logging
			self.log = logging.getLogger('pyriodic_dummy')
		self.log.info('Initializing Scheduler')

	def _set_timer(self):
		"""
		Finds the next job and if it isn't already then it will set it as the current job.
		From there it sets a timer to wait for the next scheduled time for that job to execute.
		"""
		if self.jobs:
			next_job = self.jobs[0]
			if not next_job.is_paused() and self.current_job != next_job:
				if self.sleeper:
					self.sleeper.cancel()
				wait_time = (next_job.next_run_time - now()).total_seconds()
				self.sleeper = Timer(wait_time, self._execute_job)
				self.sleeper.start()
				self.current_job = next_job
				self.running = True

	def _execute_job(self):
		"""
		Takes the currently scheduled job and starts it in it's own thread or just executes it if it is set to run
		concurrently. Then it trims, sorts and sets up the next job for execution.
		"""
		if self.current_job:
			if not self.current_job.is_paused():
				if self.current_job.threaded:
					self.current_job.thread = self.current_job.run(self.current_job.retrys)
				else:
					self.current_job.run(self.current_job.retrys)
			self.current_job = None
		if self.running:
			self._trim_jobs()
			self._sort_jobs()
			self._set_timer()

	def _sort_jobs(self):
		"""
		Sorts the jobs by the least amount of time till the next execution time to the most if there is a
		next_run_time at all
		"""
		if len(self.jobs) > 1:
			self.jobs.sort(key=lambda job: job.next_run_time if job.next_run_time is not None else datetime.max)

	def _trim_jobs(self):
		"""
		Finds any jobs that are not set to repeat and have at least executed once already to be removed from the list of jobs
		"""
		for job in self.jobs:
			if not job.repeating and job.run_count > 0:
				self.remove(job.name)

	def add_job(self, job):
		"""
		Takes a Job object and adds it to the list of jobs, gives it a name if it doesn't have one, sorts jobs and
		then sets up the next job for execution. Returns the job name for easy referencing to it later.
		"""
		if job.name is None:
			job.name = 'Job{}'.format(len(self.jobs) + 1)
		job.parent = self
		self.jobs.append(job)
		self._sort_jobs()
		self._set_timer()
		return job.name

	def schedule_job(self, job_type, when, args=None, kwargs=None, name=None, ignore_exceptions=False):
		"""
		A function wrapper to be used as a decorator to schedule jobs with
		"""
		def inner(func):
			self.add_job(job_type(func=func, args=args, kwargs=kwargs, name=name))
			return func
		return inner

	def get_job(self, name):
		"""
		Finds and returns the Job object that matched the name provided
		"""
		return self.jobs[self.find_job_index(name)]

	def reset(self):
		"""
		Resets the Scheduler status by clearing the current job,
		stopping the Timer, sorting the jobs and setting the next Timer
		"""
		self.running = True
		self.current_job = None
		if self.sleeper:
			self.sleeper.cancel()
			self.sleeper = None
		self._sort_jobs()
		self._set_timer()

	def remove(self, name):
		"""
		Finds and removes the Job object that matched the name provided
		"""
		idx = self.find_job_index(name)
		job = self.jobs[idx]
		del self.jobs[idx]
		if self.current_job == job:
			self.reset()

	def pop(self, name):
		"""
		Finds and removes the Job object that matched the name provided and returns it
		"""
		idx = self.find_job_index(name)
		if self.current_job == self.jobs[idx]:
			self.reset()
		return self.jobs.pop(idx)

	def job_names(self):
		"""
		Returns a list of the names for the jobs in the scheduler
		"""
		return [job.name for job in self.jobs]

	def find_job_index(self, name):
		"""
		Finds the index of the job that matches the name provided from the list of Job objects
		"""
		for x, job in enumerate(self.jobs):
			if job.name == name:
				return x

	def next_run_times(self):
		"""
		Returns a dictionary of the list of Job objects with the name as the key and the next run time as the item
		"""
		return {job.name: job.next_run_time for job in self.jobs}

	def start_all(self):
		"""
		Starts all the jobs if any of them happened to be paused if it isn't already running
		"""
		for job in self.jobs:
			if not job.is_running():
				job.start()
		self.reset()

	def stop_scheduler(self):
		"""
		Stops the scheduler from executing any jobs
		"""
		self.running = False
		self.current_job = None
		if self.sleeper:
			self.sleeper.cancel()
			self.sleeper = None

	def start_web_server(self, pre_existing_server=False, p=port):
		"""
		Allows for the starting of a CherryPy web application for the viewing and management of scheduled jobs.
		Requires that the user has the module CherryPy installed on their system
		"""
		global port
		try:
			# noinspection PyUnresolvedReferences
			import cherrypy
			start_web_interface(self)
			if not pre_existing_server:
				cherrypy.config.update({'server.socket_port': p})
				cherrypy.engine.start()
				print('Started the Pyriodic web interface at http://localhost:{}/pyriodic'.format(p))
				port += 1  # increments so multiple schedulers can be instantiated and display their own jobs page
		except ImportError:
			raise ImportError('The web interface requires that CherryPy be installed')