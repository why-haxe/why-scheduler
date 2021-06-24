package why.scheduler.redis;

import why.scheduler.Scheduler;

using tink.CoreApi;

class RedisScheduler<Payload> implements Scheduler<Payload> {
	final driver:RedisDriver<Payload>;
	
	public function new(driver) {
		this.driver = driver;
	}
	
	public function set(task:Task<Payload>):Promise<Noise> {
		return driver.set(task);
	}
	
	public function unset(id:String):Promise<Noise> {
		return driver.unset(id);
	}
}