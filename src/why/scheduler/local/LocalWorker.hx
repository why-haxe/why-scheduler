package why.scheduler.local;

import haxe.Timer;
import why.scheduler.Worker;

using tink.CoreApi;

@:access(why.scheduler.local)
class LocalWorker<Payload> implements Worker<Payload> {
	
	final scheduler:LocalScheduler<Payload>;
	final subscribers:Array<Pair<Filterer<Payload>, Subscriber<Payload>>>;
	
	function new(scheduler) {
		this.scheduler = scheduler;
		this.subscribers = [];
	}
	
	public function subscribe(filter:Filterer<Payload>, subscriber:Subscriber<Payload>):CallbackLink {
		final pair = new Pair(filter, subscriber);
		subscribers.push(pair);
		scheduler.runPending(this);
		return () -> subscribers.remove(pair);
	}
	
	public function destroy() {
		scheduler.removeWorker(this);
		return Future.NOISE;
	}
	
	function getSubscriber(task) {
		for(pair in subscribers) {
			final filter = pair.a;
			if(filter(task))
				return pair.b;
		}
		return null;
	}
}