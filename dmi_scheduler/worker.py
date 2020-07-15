"""
Worker class that all workers should implement
"""
import traceback
import threading
import time
import abc

from dmi_scheduler.exceptions import WorkerInterruptedException


class BasicWorker(threading.Thread, metaclass=abc.ABCMeta):
	"""
	Abstract Worker class

	This runs as a separate thread in which a worker method is executed. The
	work method can do whatever the worker needs to do - that part is to be
	implemented by a child class. This class provides scaffolding that makes
	sure crashes are caught properly and the relevant data is available to the
	worker code.
	"""
	type = "worker"  # a convenience ID for logging, interfaces, etc.
	max_workers = 1  # max amount of workers of this type

	# flag values to indicate what to do when an interruption is requested
	INTERRUPT_NONE = False
	INTERRUPT_RETRY = 1
	INTERRUPT_CANCEL = 2

	queue = None  # JobQueue
	job = None  # Job for this worker
	log = None  # Logger
	manager = None  # WorkerManager that manages this worker
	interrupted = False  # interrupt flag, to request halting
	init_time = 0  # Time this worker was started

	def __init__(self, logger, job, manager):
		"""
		Basic init, just make sure our thread name is meaningful

		:param WorkerManager manager:  Worker manager reference
		"""
		super().__init__()
		self.name = self.type
		self.log = logger
		self.manager = manager
		self.job = job
		self.init_time = int(time.time())

	def run(self):
		"""
		Loop the worker

		This simply calls the work method
		"""
		try:
			self.work()
		except WorkerInterruptedException:
			self.log.info("Worker %s interrupted" % self.type)

			# interrupted - retry later or cancel job altogether?
			if self.interrupted == self.INTERRUPT_RETRY:
				self.job.release(delay=10)
			elif self.interrupted == self.INTERRUPT_CANCEL:
				self.job.finish()

			self.abort()
		except Exception as e:
			frames = traceback.extract_tb(e.__traceback__)
			frames = [frame.filename.split("/").pop() + ":" + str(frame.lineno) for frame in frames]
			location = "->".join(frames)
			self.log.error("Worker %s raised exception %s and will abort: %s at %s" % (
			self.type, e.__class__.__name__, str(e), location))
			self.job.add_status("Crash during execution")

		self.after_work()

	def after_work(self):
		self.job.finish()

	def abort(self):
		"""
		Called when the application shuts down

		Can be used to stop loops, for looping workers.
		"""
		pass

	def request_abort(self, level=1):
		"""
		Set the 'abort requested' flag

		Child workers should quit at their earliest convenience when this is set

		:param int level:  Retry or cancel? Either `self.INTERRUPT_RETRY` or
		`self.INTERRUPT_CANCEL`.

		:return:
		"""
		self.interrupted = level

	@abc.abstractmethod
	def work(self):
		"""
		This is where the actual work happens

		Whatever the worker is supposed to do, it should happen (or be initiated from) this
		method
		"""
		pass
