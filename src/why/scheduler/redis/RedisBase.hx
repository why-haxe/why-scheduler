package why.scheduler.redis;

import tink.Chunk;
import tink.chunk.ChunkTools;

using tink.CoreApi;

class RedisBase {
	public final redis:ioredis.Redis;
	public final zkey:String;
	public final hkey:String;
	
	public function new(redisKind:RedisKind, key:String) {
		redis = switch redisKind {
			case Instance(inst): inst;
			case Options(opt): new Ioredis(opt);
		}
		zkey = key + '-time';
		hkey = key + '-payload';
		
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
	
	inline function whylist(date:Date):Promise<Array<Chunk>>
		return Promise.ofJsPromise((cast redis).whylist(zkey, hkey, date.getTime())).next((arr:Array<js.node.Buffer>) -> arr.map(Chunk.ofBuffer));
	
	inline function whyget(id:String):Promise<Bool>
		return Promise.ofJsPromise((cast redis).whyget(zkey, hkey, id)).next((v:Int) -> v == 1);
		
	inline function whyset(id:String, score:Float, data:Chunk):Promise<Noise>
		return Promise.ofJsPromise((cast redis).whyset(zkey, hkey, id, score, data.toBuffer())).noise();
		
	inline function whyunset(id:String):Promise<Noise>
		return Promise.ofJsPromise((cast redis).whyunset(zkey, hkey, id)).noise();
		
}

enum RedisKind {
	Instance(redis:ioredis.Redis);
	Options(options:ioredis.RedisOptions);
}