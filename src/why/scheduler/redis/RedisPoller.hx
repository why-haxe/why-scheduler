package why.scheduler.redis;

import why.scheduler.base.Poller;
import why.scheduler.Task;
import tink.Chunk;
import tink.chunk.ChunkTools;

using tink.CoreApi;

class RedisPoller<Payload> extends RedisBase implements Poller<Payload> {
	final unserialize:Chunk->Outcome<Payload, Error>;
	
	public function new(redisKind, key, unserialize) {
		super(redisKind, key);
		this.unserialize = unserialize;
	}
	
	public function list(date:Date):Promise<Array<Task<Payload>>> {
		return whylist(date)
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
		return whyget(id);
	}
}