from dmi_scheduler.manager import WorkerManager


class Scheduler:
	"""
	Scheduler

	Mostly a wrapper class for the WorkerManager. Because we want to be able to
	interact with the WorkerManager while it is running, it needs to live in a
	separate thread. So all this class really does is provide the scaffolding
	to start a new thread with a WorkerManager, and some choice methods to then
	interact with the manager.
	"""
	_manager = None

	def __init__(self, *args, **kwargs):
		"""
		Start WorkerManager thread

		:param args:  Will be passed on to WorkerManager.__init__()
		:param kwargs:  Will be passed on to WorkerManager.__init__()
		"""
		self._manager = WorkerManager(*args, **kwargs)
		self._manager.start()

	def has_jobs(self):
		"""
		Are there any jobs still in the WorkerManager's queue?

		:return bool:
		"""
		return len(self._manager.queue.get_all_jobs()) > 0

	def end(self):
		"""
		End WorkerManager main loop

		This should be called to terminate the WorkerManager thread. Until this
		is called, the queue will be watched for new jobs to start.
		"""
		self._manager.abort()

	@property
	def queue(self):
		"""
		Return job queue object

		:return JobQueue:
		"""
		return self._manager.queue

	@property
	def log(self):
		"""
		Return logger object

		:return:
		"""
		return self._manager._log
