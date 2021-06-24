package why.scheduler.local;

import haxe.Timer;
import why.scheduler.Worker;

using tink.CoreApi;

@:access(why.scheduler.local)
class LocalWorker<Payload> implements Worker<Payload> {
	
	final scheduler:LocalScheduler<Payload>;
	final subscribers:Array<Pair<Subscriber<Payload>, Filterer<Payload>>>;
	
	function new(scheduler) {
		this.scheduler = scheduler;
		this.subscribers = [];
	}
	
	public function subscribe(subscriber:Subscriber<Payload>, ?options:SubscribeOptions<Payload>):CallbackLink {
		final pair = new Pair(subscriber, switch options {
			case null | {filter: null}: Filterer.allow;
			case {filter: f}: f;
		});
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
			final filter = pair.b;
			if(filter(task))
				return pair.a;
		}
		return null;
	}
}