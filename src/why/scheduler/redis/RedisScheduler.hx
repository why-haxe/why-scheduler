package why.scheduler.redis;

import why.scheduler.Scheduler;
import tink.Chunk;

using tink.CoreApi;

class RedisScheduler<Payload> extends RedisBase implements Scheduler<Payload> {
	final serialize:Payload->Chunk;
	
	public function new(redis, key, serialize) {
		super(redis, key);
		this.serialize = serialize;
		
		// set: zkey, hkey | taskid, time, data
		this.redis.defineCommand('whyset', {
			numberOfKeys: 2,
			lua: '
				redis.call("zadd", KEYS[1], ARGV[2], ARGV[1])
				redis.call("hset", KEYS[2], ARGV[1], ARGV[3])
			',
		});
		
		// set: zkey, hkey | taskid
		this.redis.defineCommand('whyunset', {
			numberOfKeys: 2,
			lua: '
				redis.call("zrem", KEYS[1], ARGV[1])
				redis.call("hdel", KEYS[2], ARGV[1])
			',
		});
	}
	
	public function set(task:Task<Payload>):Promise<Noise> {
		return Promise.ofJsPromise((cast redis).whyset(zkey, hkey, task.id, task.at.getTime(), serialize(task.payload).toBuffer()));
	}
	
	public function unset(id:String):Promise<Noise> {
		return Promise.ofJsPromise((cast redis).whyunset(zkey, hkey, id));
	}
}