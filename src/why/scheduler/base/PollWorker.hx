package why.scheduler.base;

import haxe.Timer;
import why.scheduler.Worker;
import why.scheduler.Task;

using tink.CoreApi;

class PollWorker<Payload> implements Worker<Payload> {
	final poller:Poller<Payload>;
	final interval:Int;
	final subscriptions:Array<Subscription<Payload>> = [];
	final binding:CallbackLink;
	
	public function new(poller, interval = 1000) {
		this.poller = poller;
		this.interval = interval;
		this.binding = monitor();
	}
	
	
	public function subscribe(subscriber:Subscriber<Payload>, ?options:SubscribeOptions<Payload>):CallbackLink {
		final subscription = new Subscription(subscriber, options);
		subscriptions.push(subscription);
		return () -> subscriptions.remove(subscription);
	}
	
	public function destroy() {
		binding.cancel();
		return Future.NOISE; // TODO: wait for all subscriber finish
	}
	
	function monitor() {
		var running = true;
		
		function poll() {
			if(!running) return;
			
			inline function next(delay)
				Timer.delay(poll, delay);
			
			poller.list(Date.now()).handle(function(o) switch o {
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
									poller.get(task.id)
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
		}
		
		Callback.defer(poll); // start polling on next tick
		
		return () -> running = false;
	}
}