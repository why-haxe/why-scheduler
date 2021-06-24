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
	
	final driver:RedisDriver<Payload>;
	
	public function new(driver, ?options:RedisWorkerOptions) {
		super(switch options {
			case null: null;
			case {interval: v}: v;
		});
		
		this.driver = driver;
	}
	
	function list():Promise<Array<Task<Payload>>> {
		return driver.list(Date.now().getTime());
	}
	
	function get(id:String):Promise<Bool> {
		return driver.get(id);
	}
}
