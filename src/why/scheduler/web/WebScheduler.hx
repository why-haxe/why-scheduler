package why.scheduler.web;

import why.scheduler.Scheduler;

using tink.CoreApi;

class WebScheduler<Payload> implements Scheduler<Payload> {
	final remote:Interface<Payload>;
	
	public function new(remote) {
		this.remote = remote;
	}
	
	public function set(task:Task<Payload>):Promise<Noise> {
		return remote.set(task);
	}
	
	public function unset(id:String):Promise<Noise> {
		return remote.unset(id);
	}
	
}