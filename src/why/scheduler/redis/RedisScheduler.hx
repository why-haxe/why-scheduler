package why.scheduler.redis;

import why.scheduler.Scheduler;
import tink.Chunk;
import tink.chunk.ChunkTools;

using tink.CoreApi;

class RedisScheduler<Payload> extends RedisBase implements Scheduler<Payload> {
	final serialize:Payload->Chunk;
	
	public function new(redisKind, key, serialize) {
		super(redisKind, key);
		this.serialize = serialize;
		
		// set: zkey, hkey | taskid, time, data
		redis.defineCommand('whyset', {
			numberOfKeys: 2,
			lua: '
				redis.call("zadd", KEYS[1], ARGV[2], ARGV[1])
				redis.call("hset", KEYS[2], ARGV[1], ARGV[3])
			',
		});
		
		// set: zkey, hkey | taskid
		redis.defineCommand('whyunset', {
			numberOfKeys: 2,
			lua: '
				redis.call("zrem", KEYS[1], ARGV[1])
				redis.call("hdel", KEYS[2], ARGV[1])
			',
		});
	}
	
	public function set(task:Task<Payload>):Promise<Noise> {
		return Promise.ofJsPromise((cast redis).whyset(zkey, hkey, task.id, task.window.from.getTime(), makeRedisPayload(task).toBuffer()));
	}
	
	public function unset(id:String):Promise<Noise> {
		return Promise.ofJsPromise((cast redis).whyunset(zkey, hkey, id));
	}
	
	inline function makeRedisPayload(task:Task<Payload>):Chunk {
		return ChunkTools.writeDoubleLE(switch task.window.to {
			case null: Math.NaN;
			case v: v.getTime();
		}) & serialize(task.payload);
	}
}