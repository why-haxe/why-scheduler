package why.scheduler.redis;

import why.scheduler.Task;
import tink.Chunk;
import tink.chunk.ChunkTools;

using tink.CoreApi;
class RedisDriver<Payload> {
	public final redis:ioredis.Redis;
	public final zkey:String;
	public final hkey:String;
	
	final serialize:Payload->Chunk;
	final unserialize:Chunk->Outcome<Payload, Error>;
	
	public function new(redisKind:RedisKind, key:String, serialize, unserialize) {
		redis = switch redisKind {
			case Instance(inst): inst;
			case Options(opt): new Ioredis(opt);
		}
		zkey = key + '-time';
		hkey = key + '-payload';
		this.serialize = serialize;
		this.unserialize = unserialize;
		
		// list: zkey, hkey | time
		redis.defineCommand('whylist', {
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
		redis.defineCommand('whyget', {
			numberOfKeys: 2,
			lua: '
				local result = redis.call("zrem", KEYS[1], ARGV[1])
				if(result == 1) then
					redis.call("hdel", KEYS[2], ARGV[1])
				end
				return result
			',
		});
		
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
	
	public function list(time:Float):Promise<Array<Task<Payload>>> {
		return Promise.ofJsPromise((cast redis).whylist(zkey, hkey, Date.now().getTime()))
			.next(arr -> {
				Promise.inParallel([for(i in 0...Std.int(arr.length / 3)) {
					final id = arr[i * 3 + 0].toString();
					final time = Std.parseFloat(arr[i * 3 + 1].toString());
					final raw = arr[i * 3 + 2]; // first 8 bytes of payload is used to store the expiry date (NaN if no expiry)
					
					unserialize(raw.slice(8, raw.length)).map(payload -> {
						id: id,
						window: ({
							from: Date.fromTime(time),
							to: try {
								final time = raw.readDoubleLE(0);
								Math.isNaN(time) ? null : Date.fromTime(time);
							} catch(e) null,
						}:Window),
						payload: payload,
					});
				}]);
			});
	}
			
	public function get(id:String):Promise<Bool> {
		return Promise.ofJsPromise((cast redis).whyget(zkey, hkey, id)).next(v -> v == 1);
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

enum RedisKind {
	Instance(redis:ioredis.Redis);
	Options(options:ioredis.RedisOptions);
}