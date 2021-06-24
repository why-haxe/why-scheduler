package why.scheduler.redis;

import why.scheduler.base.PollWorker;
import why.scheduler.Task;

using tink.CoreApi;

typedef RedisWorkerOptions = {
	?interval:Int,
}

class RedisWorker<Payload> extends PollWorker<Payload> {
	
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
