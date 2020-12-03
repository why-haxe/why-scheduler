package why.scheduler.redis;

import haxe.Timer;
import why.scheduler.Worker;

using tink.CoreApi;

class RedisWorker<Payload> extends RedisBase implements Worker<Payload> {
	final unserialize:String->Outcome<Payload, Error>;
	
	final interval:Int;
	final maxConcurrency:Int;
	final subscribers:Array<Subscriber<Payload>> = [];
	
	var concurrency = 0;
	var binding:CallbackLink = null;
	
	public function new(kind, key, unserialize, interval = 1000, maxConcurrency = 5) {
		super(kind, key);
		this.unserialize = unserialize;
		this.interval = interval;
		this.maxConcurrency = maxConcurrency;
	}
	
	public function subscribe(s:Subscriber<Payload>):CallbackLink {
		add(s);
		return remove.bind(s);
	}
	
	inline function add(s:Subscriber<Payload>) {
		if(subscribers.length == 0) {
			binding = monitor();
		}
		subscribers.push(s);
	}
	
	function remove(s:Subscriber<Payload>) {
		final removed = subscribers.remove(s);
		if(removed && subscribers.length == 0) {
			binding.cancel();
		}
	}
	
	inline function monitor() {
		var running = true;
		
		(function poll() {
			if(!running) return;
			
			inline function next(delay) Timer.delay(poll, delay);
			
			// consider using bzpopmin
			Promise.ofJsPromise(redis.zpopmin(zkey)).handle(function(o) switch o {
				case Success([]):
					// set is empty, try again later
					next(interval);
					
				case Success([member, score]):
					
					final id = member;
					final time = Std.parseFloat(score);
					final dt = Std.int(Date.now().getTime() - time);
					
					if(dt >= 0) {
						Promise.ofJsPromise(redis.hget(hkey, id))
							.next(unserialize)
							.handle(o -> {
								redis.hdel(hkey, id);
								switch o {
									case Success(payload):
										final task:Task<Payload> = {id: id, at: Date.fromTime(time), payload: payload}
										Future #if (tink_core < "2") .ofMany #else .inParallel #end ([for(s in subscribers) s(task)])
											.handle(_ -> if(concurrency-- == maxConcurrency) next(0));
										
										// poll again if we have the capacity
										if(++concurrency < maxConcurrency)
											next(0);
										
									case Failure(e):
										// just forget about it if we can't get or decode the payload, because there is nothing we can do
										next(interval);
								}
							});
					} else {
						// put back because it is not yet ready
						redis.zadd(zkey, time, id);
						next(-dt < interval ? -dt : interval);
					}
				case Success(_):
					next(interval); // unreachable as we only request one item
				case Failure(e):
					trace(e);
					next(interval); // redis error
			});
		})();
		
		return () -> running = false;
	}
}