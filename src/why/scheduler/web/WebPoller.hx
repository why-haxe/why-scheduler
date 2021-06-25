package why.scheduler.web;

import why.scheduler.base.Poller;
import why.scheduler.Task;

using tink.CoreApi;

class WebPoller<Payload> implements Poller<Payload> {
	final remote:Interface<Payload>;
	
	public function new(remote) {
		this.remote = remote;
	}
	
	public function list(date:Date):Promise<Array<Task<Payload>>> {
		return remote.list(date);
	}
	
	public function get(id:String):Promise<Bool> {
		return remote.get(id);
	}
}