package why.scheduler.redis;

import haxe.Timer;
import why.scheduler.Worker;
import why.scheduler.Task;
import tink.Chunk;

using tink.CoreApi;

typedef RedisWorkerOptions = {
	?interval:Int,
}

class RedisWorker<Payload> extends why.scheduler.base.PollWorker<Payload> {
	final unserialize:Chunk->Outcome<Payload, Error>;
	
	final driver:RedisDriver;
	
	public function new(driver, unserialize, ?options:RedisWorkerOptions) {
		super(switch options {
			case null: null;
			case {interval: v}: v;
		});
		
		this.driver = driver;
		this.unserialize = unserialize;
		
		// list: zkey, hkey | time
		driver.redis.defineCommand('whylist', {
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
		driver.redis.defineCommand('whyget', {
			numberOfKeys: 2,
			lua: '
				local result = redis.call("zrem", KEYS[1], ARGV[1])
				if(result == 1) then
					redis.call("hdel", KEYS[2], ARGV[1])
				end
				return result
			',
		});
	}
			
	inline function whylist(time:Float):js.lib.Promise<Array<js.node.Buffer>> {
		return (cast driver.redis).whylist(driver.zkey, driver.hkey, Date.now().getTime());
	}
			
	inline function whyget(id:String):js.lib.Promise<Int> {
		return (cast driver.redis).whyget(driver.zkey, driver.hkey, id);
	}
	
	function list():Promise<Array<Task<Payload>>> {
		return Promise.ofJsPromise(whylist(Date.now().getTime())).next(arr -> {
			Promise.inParallel([for(i in 0...Std.int(arr.length / 3)) {
				final id = arr[i * 3 + 0].toString();
				final time = Std.parseFloat(arr[i * 3 + 1].toString());
				final raw = arr[i * 3 + 2]; // first 8 bytes of payload is used to store the expiry date (NaN if no expiry)
				
				unserialize(raw.slice(8)).map(payload -> {
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
	
	function get(id:String):Promise<Bool> {
		return Promise.ofJsPromise(whyget(id)).next(v -> v != 0);
	}
}
