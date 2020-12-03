package why.scheduler.redis;

import why.scheduler.Scheduler;

using tink.CoreApi;

class RedisScheduler<Payload> extends RedisBase implements Scheduler<Payload> {
	final serialize:Payload->String;
	
	public function new(kind, key, serialize) {
		super(kind, key);
		this.serialize = serialize;
	}
	
	public function set(task:Task<Payload>):Promise<Noise> {
		return Promise.inParallel([
			Promise.ofJsPromise(redis.zadd(zkey, task.at.getTime(), task.id)).noise(),
			Promise.ofJsPromise(redis.hset(hkey, task.id, serialize(task.payload))).noise(),
		]);
	}
	
	public function unset(id:String):Promise<Noise> {
		return Promise.inParallel([
			Promise.ofJsPromise(redis.zrem(zkey, id)).noise(),
			Promise.ofJsPromise(redis.hdel(hkey, id)).noise(),
		]);
	}
}