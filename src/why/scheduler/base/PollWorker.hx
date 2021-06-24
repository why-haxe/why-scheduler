package why.scheduler.base;

import haxe.Timer;

abstract class PollWorker<Payload> {
	final interval:Int;
	
	public function new(interval) {
		this.interval = interval;
	}
	
	/**
	 * List tasks that are ready to be handled
	 * @return Promise<Array<Task<Payload>>>
	 */
	 abstract function list():Promise<Array<Task<Payload>>>;
	 
	/**
	 * Get a task atomically 
	 * @param id 
	 * @return Promise<Bool> true if successfully obtained ownership
	 */
	abstract function get(id:String):Promise<Bool>;
	
	
	function monitor() {
		var running = true;
		
		(function poll() {
			if(!running) return;
			
			inline function next(delay)
				Timer.delay(poll, delay);
			
			list().handle(function(o) switch o {
				case Success([]):
					// set is empty, try again later
					next(interval);
					
				case Success(tasks):
					for(task in tasks) {
						for(subscription in subscriptions) {
							var handled = false;

							switch subscription.semaphore.tryAcquire() {
								case Some({b: lock}) if(subscription.filter(task)):
									handled = true;
									
									// able to handle, try to acquire the task
									get(id)
										.handle(function(o) switch o {
											case Success(false):
												// can't acquire
												lock.cancel(); // release lock
											case Success(true):
												// locked item
												subscription.subscriber(task).handle(lock);
													
											case Failure(e):
												// hmm...
												trace(e);
												lock.cancel(); // release lock
										});
									
								case _:
									// skip
							}
								
							if(handled) break; // only handle once per task
						}
					}
					
					// reaching here means we have processed all tasks and still have quota
					// so wait for some time and poll again
					next(interval);
					
				case Failure(e):
					trace(e);
					next(interval); // redis error
			});
		})();
		
		return () -> running = false;
	}
}