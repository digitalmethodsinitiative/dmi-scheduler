# dmi-scheduler

A lightweight task scheduler in Python.


Example:

```
import time
from scheduler.scheduler import Scheduler

scheduler = Scheduler(config="scheduler.yml")
for i in range(0, 100):
    scheduler.queue.add_job("log_number.py")

while scheduler.has_jobs():
    time.sleep(1)

scheduler.end()
print("Done! Results can be found in the log file.") 
```

some_python_file.py:
```
from random import choice
from scheduler.worker import BasicWorker

class SomeWorker(BasicWorker):
	max_workers = 3

	def work(self):
		self.log.info("Here is a random number: %i" % random.choice(range(0,1000)))
```