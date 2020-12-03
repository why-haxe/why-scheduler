// package why.scheduler.local;

// import haxe.Timer;
// import why.scheduler.Worker;

// using tink.CoreApi;

// class LocalWorker<Payload> implements Worker<Payload> {
// 	public var ready(get, never):Bool;
	
// 	final maxConcurrency:Int;
// 	var concurrency = 0;
	
// 	final subscribers:Array<Subscriber<Payload>> = [];
// 	final scheduler:LocalScheduler<Payload>;
	
// 	public function new(scheduler, maxConcurrency) {
// 		this.scheduler = scheduler;
// 		this.maxConcurrency = maxConcurrency;
// 	}
	
// 	public function subscribe(s:Subscriber<Payload>):CallbackLink {
// 		add(s);
// 		return remove.bind(s);
// 	}
	
// 	inline function add(s:Subscriber<Payload>) {
// 		if(subscribers.length == 0) {
// 			binding = start();
// 		}
// 		subscribers.push(s);
// 	}
	
// 	function remove(s:Subscriber<Payload>) {
// 		final removed = subscribers.remove(s);
// 		if(removed && subscribers.length == 0) {
// 			binding.cancel();
// 		}
// 	}
	
// 	inline function start() {
// 		var running = true;
		
// 		(function poll() {
// 			if(!running) return;
// 			while(concurrency < maxConcurrency) {
// 				switch scheduler.poll() {
// 					case null:
// 						break;
// 					case task:
// 						concurrency++;
// 						Future.inParallel([for(s in subscribers) s(task)]).handle(_ -> if(concurrency-- == maxConcurrency) poll());
// 				}
// 			}
// 		})();
		
// 		return () -> running = false;
// 	}
	
// 	public function run():Future<Bool> { // resolves true if available again
// 		concurrency++;
// 		Future.inParallel([for(s in subscribers) s(task)]).map(_ -> concurrency-- == maxConcurrency);
// 	}
	
// 	inline function get_ready() {
// 		return concurrency < maxConcurrency && subscribers.length > 0;
// 	}
// }