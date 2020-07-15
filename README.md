# dmi-scheduler

A lightweight task scheduler in Python.

## Installation
```
pip3 install dmi-scheduler
```

PostgreSQL is used to keep track of jobs. As such, you need to have a 
PostgreSQL database that the scheduler can interact with. Database 
tables will be created automatically if they don't exist yet. You can
pass the database connection parameters to the `Scheduler()` 
constructor with the `dbname`, `dbhost`, `dbuser`, `dbpassword` and
`dbport` keyword arguments. 

## Example

```
import time
from dmi_scheduler.scheduler import Scheduler

scheduler = Scheduler(config="scheduler.yml")
for i in range(0, 100):
    scheduler.queue.add_job("log_number.py")

while scheduler.has_jobs():
    time.sleep(1)

scheduler.end()
print("Done! Results can be found in the log file.") 
```

`log_number.py`:
```
from random import choice
from dmi_scheduler.worker import BasicWorker

class SomeWorker(BasicWorker):
	max_workers = 3

	def work(self):
		self.log.info("Here is a random number: %i" % choice(range(0,1000)))
```

## License
This scraper was developed by the 
[Digital Methods Initiative](https://digitalmethods.net), and is distributed
under the MIT license. See LICENSE for details.