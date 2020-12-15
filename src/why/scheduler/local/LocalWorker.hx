package why.scheduler.local;

import haxe.Timer;
import why.scheduler.Worker;

using tink.CoreApi;

@:access(why.scheduler.local)
class LocalWorker<Payload> implements Worker<Payload> {
	
	final scheduler:LocalScheduler<Payload>;
	final subscriber:Subscriber<Payload>;
	
	public function new(scheduler, subscriber) {
		this.scheduler = scheduler;
		this.subscriber = subscriber;
	}
	
	public function destroy() {
		scheduler.removeWorker(this);
		return Future.NOISE;
	}
	
	public function run(task) {
		return subscriber(task);
	}
}