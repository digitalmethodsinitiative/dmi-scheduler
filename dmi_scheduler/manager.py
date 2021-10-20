"""
The heart of the app - manages jobs and workers
"""
import threading
import logging
import inspect
import yaml
import time
import sys
import re

from pathlib import Path
from types import ModuleType
from importlib.machinery import SourceFileLoader
from logging.handlers import RotatingFileHandler

from dmi_scheduler.worker import BasicWorker
from dmi_scheduler.queue import JobQueue
from dmi_scheduler.database import Database
from dmi_scheduler.exceptions import JobClaimedException


class WorkerManager(threading.Thread):
	"""
	Manages the job queue and worker pool
	"""
	queue = None
	_db = None
	_log = None

	looping = True
	_worker_map = {}
	_worker_pool = {}
	_job_mapping = {}

	def __init__(self, *args, **kwargs):
		"""
		Initialize manager

		Parameters can be provided via keyword arguments or via a
		YAML-formatted configuration file. If both are used, the configuration
		file will take precedence.

		:param config: Path to a configuration file (yaml-formatted). If
		provided, parameters will be read from this file, and they will
		take precedence over constructor arguments. Optional.

		:param logger: A logging object implementing the standard logger
		interface defined in PEP 282. If not provided, a rotating log file
		handler will be used. This parameter cannot be defined via the
		configuration file. Optional.

		:param str logfile:  When using the default rotating logger, this
		determines the path where the log file will be stored. Defaults to
		'scheduler.log'.

		:param int logsize:  When using the default rotating logger, this
		determines how big the log file can be before rotating. Defaults
		to 50MB.

		:param int logcount:  When using the default rotating logger, this
		determines how many log files to keep. Defaults to 1.

		:param str logformat:  When using the default rotating logger, this
		determines the format. Defaults to "%(asctime)s [%(levelname)-5.5s]
		%(message)s" and uses logging.Formatter().

		:param int loglevel: When using the default rotating logger, this
		determines what level of log message to save in the log. Defaults to
		logging.INFO.

		:param str dbname:  Name of the PostgreSQL database used to store the
		job queue.

		:param str dbuser:  Username for the PostgreSQL database used to store
		the job queue.

		:param str dbpassword:  Password for the PostgreSQL database used to
		store the job queue.

		:param str dbhost:  Location of the PostgreSQL database used to store
		the job queue. Defaults to localhost.

		:param str dbport:  Port of the PostgreSQL database used to store
		the job queue. Defaults to 5432.
		"""
		super(WorkerManager, self).__init__()

		# load configuration from external file, if available
		if "config" in kwargs:
			config_path = Path(kwargs["config"])
			if not config_path.exists():
				raise RuntimeError("Configuration file %s not found" % config_path)

			with config_path.open("r") as config_stream:
				config = yaml.safe_load(config_stream)
		else:
			config = {}

		# if no logger is provided, instantiate a standard rotating log file
		# which may be customised somewhat via parameters
		if "logger" not in kwargs:
			logfile = config.get("logfile", kwargs.get("logfile", "scheduler.log"))
			handler = RotatingFileHandler(
				filename=logfile,
				maxBytes=int(config.get("logsize", kwargs.get("logsize", 50 * 1024 * 1024))),
				backupCount=config.get("logcount", kwargs.get("logcount", 1))
			)
			logformat = config.get("logformat", kwargs.get("logformat", "%(asctime)s [%(levelname)-5.5s]  %(message)s"))
			handler.setFormatter(logging.Formatter(logformat))

			self._log = logging.getLogger("dmi-scheduler")
			self._log.setLevel(config.get("loglevel", kwargs.get("loglevel", logging.INFO)))
			self._log.addHandler(handler)
		else:
			self._log = kwargs.get("logger")

		# instantiate database handler - login _needs_ to be supplied via
		# either constructor args or config file
		self._db = Database(
			logger=self._log,
			dbname=config.get("dbname", kwargs.get("dbname")),
			user=config.get("dbuser", kwargs.get("dbuser")),
			password=config.get("dbpassword", kwargs.get("dbpassword")),
			host=config.get("dbhost", kwargs.get("dbhost", "localhost")),
			port=config.get("dbport", kwargs.get("dbport", 5432))
		)

		self.database_setup()

		# with the database and logger, we can instantiate a queue
		self.queue = JobQueue(logger=self._log, database=self._db)

	# it's time

	def database_setup(self):
		"""
		Make sure database tables exist
		"""
		with Path(__file__).parents[0].joinpath("database.sql").open() as dbfile:
			self._db.execute(dbfile.read())

	def delegate(self):
		"""
		Delegate work

		Checks for open jobs, and then passes those to dedicated workers, if
		slots are available for those workers.
		"""
		jobs = self.queue.get_all_jobs()

		num_active = sum([len(self._worker_pool[pythonfile]) for pythonfile in self._worker_pool])
		self._log.debug("Running workers: %i" % num_active)

		# clean up workers that have finished processing
		for pythonfile in self._worker_pool:
			all_workers = self._worker_pool[pythonfile]
			for worker in all_workers:
				if not worker.is_alive():
					worker.join()
					self._worker_pool[pythonfile].remove(worker)

			del all_workers

		# check if workers are available for unclaimed jobs
		for job in jobs:
			pythonfile = Path(job.data["pythonfile"])

			# does the worker script actually exist?
			if not pythonfile.exists():
				job.add_status("Job script does not exist. Cancelling.")
				job.finish()
				continue

			# import from arbitrary source file
			# module name will be based on file path - e.g. home.sam.4cat.workers.some_worker
			worker_class = self.get_worker_for_file(pythonfile)
			worker_type = self.get_worker_type_for_file(pythonfile)
			if not worker_class:
				continue

			if worker_type not in self._worker_pool:
				self._worker_pool[worker_type] = []

			if len(self._worker_pool[worker_type]) >= worker_class.max_workers:
				# already at max concurrent workers of this type
				continue

			try:
				job.claim()
				worker = worker_class(logger=self._log, manager=self, job=job)
				worker.start()
				self._worker_pool[worker_type].append(worker)
			except JobClaimedException:
				continue

		time.sleep(0.1)

	def get_worker_type_for_file(self, pythonfile):
		"""
		Generate a module name for an arbitrary Python file

		Modules need a name... so convert the absolute file path to one. Here
		the following:

		/home/sam/pythonfiles/scripts/generate-something.py

		Becomes:

		home.sam.pythonfiles.scripts.generate_something

		:param Path pythonfile:  Path to file to generate module name for
		:return str:  Module name
		"""
		# make path absolute (though it realistically should be absolute already)
		pythonfile = pythonfile.resolve()

		worker_type = re.sub(r"[^0-9a-zA-Z._]", "", ".".join(pythonfile.parts).replace("-", "_"))
		worker_type = re.sub(r"\.+", ".", worker_type)
		worker_type = re.sub(r"^\.+", "", worker_type).lower()
		return worker_type

	def get_worker_for_file(self, pythonfile):
		"""
		Return a worker class for a given Python file

		If the file contains no class that descends BasicWorker and can be
		instantiated as a worker, `None` is returned instead. Results are
		cached.

		:param pythonfile:  Path to a python file containing a worker class
		:return:  Class that can be instantiated as a worker
		"""
		worker_type = self.get_worker_type_for_file(pythonfile)

		if worker_type in self._worker_map:
			return self._worker_map[worker_type]

		if worker_type not in sys.modules:
			loader = SourceFileLoader(worker_type, str(pythonfile))
			worker_module = ModuleType(loader.name)
			loader.exec_module(worker_module)
			sys.modules[worker_type] = worker_module

		for component in inspect.getmembers(sys.modules[worker_type]):
			if component[0][0:2] != "__" \
					and inspect.isclass(component[1]) \
					and (issubclass(component[1], BasicWorker) or issubclass(component[1], BasicWorker)) \
					and not inspect.isabstract(component[1]):
				self._worker_map[worker_type] = component[1]
				return self._worker_map[worker_type]

		return None

	def run(self):
		"""
		Main loop

		Constantly delegates work, until no longer looping, after which all
		workers are asked to stop their work. Once that has happened, the loop
		properly ends.
		"""
		while self.looping:
			self.delegate()

		self._log.info("Telling all workers to stop doing whatever they're doing...")
		for pythonfile in self._worker_pool:
			for worker in self._worker_pool[pythonfile]:
				if hasattr(worker, "request_abort"):
					worker.request_abort()
				else:
					worker.abort()

		# wait for all workers to finish
		self._log.info("Waiting for all workers to finish...")
		for pythonfile in self._worker_pool:
			for worker in self._worker_pool[pythonfile]:
				self._log.info("Waiting for worker %s..." % pythonfile)
				worker.join()

		time.sleep(3)

		# abort
		self._log.info("Bye!")

	def abort(self, signal=None, stack=None):
		"""
		Stop looping the delegator, clean up, and prepare for shutdown
		"""
		self._log.info("Ending main loop")

		# now stop looping (i.e. accepting new jobs)
		self.looping = False

	def request_interrupt(self, job, interrupt_level):
		"""

		:param Job job:
		:return:
		"""

		# find worker for given job
		if job.data["pythonfile"] not in self._worker_pool:
			# no jobs of this type currently known
			return

		for worker in self._worker_pool[job.data["pythonfile"]]:
			if worker.job.data["id"] == job.data["id"]:
				worker.request_abort(interrupt_level)
				return
