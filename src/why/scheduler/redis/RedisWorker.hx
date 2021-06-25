package why.scheduler.redis;

import why.scheduler.base.PollWorker;
import why.scheduler.Worker;
import why.scheduler.Task;

using tink.CoreApi;

typedef RedisWorkerOptions = {
	?interval:Int,
}

class RedisWorker<Payload> implements Worker<Payload> {
	
	final worker:PollWorker<Payload>;
	
	public function new(poller, ?options:RedisWorkerOptions) {
		worker = new PollWorker(poller, switch options {
			case null: null;
			case {interval: v}: v;
		});
	}
	
	public function subscribe(subscriber:Subscriber<Payload>, ?options:SubscribeOptions<Payload>):CallbackLink {
		return worker.subscribe(subscriber, options);
	}
	
	public function destroy() {
		return worker.destroy();
	}
}
