package why.scheduler.redis;

import haxe.Timer;
import why.scheduler.Worker;
import why.scheduler.Quota;
import tink.Chunk;

using tink.CoreApi;

typedef RedisWorkerOptions = {
	?interval:Int,
	?concurrency:Int,
}

class RedisWorker<Payload> extends RedisBase implements Worker<Payload> {
	final unserialize:Chunk->Outcome<Payload, Error>;
	
	final interval:Int;
	final quota:Quota;
	final subscribers:Array<Pair<Filterer<Payload>, Subscriber<Payload>>>;
	final binding:CallbackLink;
	
	public function new(redis, key, unserialize, ?options:RedisWorkerOptions) {
		super(redis, key);
		this.subscribers = [];
		this.unserialize = unserialize;
		this.interval = options != null && options.interval != null ? options.interval : 1000;
		this.quota = new Quota(options != null && options.concurrency != null ? options.concurrency : 5);
		
		// list: zkey, hkey | time
		this.redis.defineCommand('whylist', {
			numberOfKeys: 2,
			lua: '
				local list = redis.call("zrangebyscore", KEYS[1], 0, ARGV[1], "WITHSCORES")
				local i = 0
				local total = #list / 2
				local ret = {}
				while (i < total) do 
					local member = list[i * 2 + 1]
					local score = list[i * 2 + 2]
					local data = redis.call("hget", KEYS[2], member)
					ret[i * 3 + 1] = member
					ret[i * 3 + 2] = score
					ret[i * 3 + 3] = data
					i = i + 1
				end
				return ret;
			',
		});
		
		// list: zkey, hkey | taskid
		this.redis.defineCommand('whyget', {
			numberOfKeys: 2,
			lua: '
				local result = redis.call("zrem", KEYS[1], ARGV[1])
				if(result == 1) then
					redis.call("hdel", KEYS[2], ARGV[1])
				end
				return result
			',
		});
		
		
		this.binding = monitor();
	}
	
	public function subscribe(filter:Filterer<Payload>, subscriber:Subscriber<Payload>):CallbackLink {
		final pair = new Pair(filter, subscriber);
		subscribers.push(pair);
		return () -> subscribers.remove(pair);
	}
	
	public function destroy() {
		binding.cancel();
		return Future.NOISE; // TODO: wait for all subscriber finish
	}
			
	inline function whylist(time:Float):js.lib.Promise<Array<js.node.Buffer>> {
		return (cast redis).whylist(zkey, hkey, Date.now().getTime());
	}
			
	inline function whyget(id:String):js.lib.Promise<Int> {
		return (cast redis).whyget(zkey, hkey, id);
	}
	
	inline function monitor() {
		var running = true;
		
		(function poll() {
			if(!running) return;
			
			inline function next(delay) Timer.delay(poll, delay);
			
			// TODO: perhaps should peek first to avoid writes (zrange)
			// TODO: consider using bzpopmin
			Promise.ofJsPromise(whylist(Date.now().getTime())).handle(function(o) switch o {
				case Success([]):
					// set is empty, try again later
					next(interval);
					
				case Success(arr):
					for(i in 0...Std.int(arr.length / 3)) {
						final id = arr[i * 3 + 0].toString();
						final time = Std.parseFloat(arr[i * 3 + 1].toString());
						
						switch unserialize(arr[i * 3 + 2]) {
							case Success(payload):
								final task:Task<Payload> = {id: id, at: Date.fromTime(time), payload: payload};
								
								for(pair in subscribers) {
									final filter = pair.a;
									final subscriber = pair.b;
									if(filter(task)) {
										switch quota.acquire() {
											case Some(lock):
												// able to handle, try to acquire the task
												Promise.ofJsPromise(whyget(id))
													.handle(function(o) switch o {
														case Success(0):
															// can't acquire
															lock.cancel(); // release lock
														case Success(v):
															// locked item
															subscriber(task).handle(lock);
																
														case Failure(e):
															// hmm...
															trace(e);
															lock.cancel(); // release lock
													});
												
											case None:
												quota.available.handle(poll);
												return; // end this poll
										}
										break; // only one subscriber can process
									}
								}
							case Failure(e):
								trace(e);
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
