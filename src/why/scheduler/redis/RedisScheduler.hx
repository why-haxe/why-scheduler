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
	}
	
	public function set(task:Task<Payload>):Promise<Noise> {
		return whyset(task.id, task.window.from.getTime(), makeRedisPayload(task));
	}
	
	public function unset(id:String):Promise<Noise> {
		return whyunset(id);
	}
	
	inline function makeRedisPayload(task:Task<Payload>):Chunk {
		return ChunkTools.writeDoubleLE(switch task.window.to {
			case null: Math.NaN;
			case v: v.getTime();
		}) & serialize(task.payload);
	}
}