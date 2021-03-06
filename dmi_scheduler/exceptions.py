class SchedulerException(Exception):
	"""
	General exception - to be descended from
	"""
	pass


class QueueException(SchedulerException):
	"""
	General Queue Exception - only children are to be used
	"""
	pass


class JobClaimedException(QueueException):
	"""
	Raise if job is claimed, but is already marked as such
	"""
	pass


class JobAlreadyExistsException(QueueException):
	"""
	Raise if a job is created, but a job with the same type/remote_id combination already exists
	"""
	pass


class JobNotFoundException(QueueException):
	"""
	Raise if trying to instantiate a job with an ID that is not valid
	"""
	pass


class WorkerInterruptedException(SchedulerException):
	"""
	Raise when killing a worker before it's done with its job
	"""
	pass


class DatabaseQueryInterruptedException(SchedulerException):
	pass
