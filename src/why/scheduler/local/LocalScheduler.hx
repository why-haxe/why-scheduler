package why.scheduler.local;

import haxe.Timer;
import why.scheduler.Scheduler;

using tink.CoreApi;

class LocalScheduler<Payload> implements Scheduler<Payload> {
	
	final timers:Map<String, Timer> = [];
	final workers:Array<LocalWorker<Payload>> = [];
	final pending:Array<Task<Payload>> = [];
	
	var index:Int = 0;
	
	public function new() {}
	
	public function set(task:Task<Payload>):Promise<Noise> {
		switch timers[task.id] {
			case null:
			case timer: timer.stop();
		}
		
		timers[task.id] = Timer.delay(run.bind(task), Std.int(task.at.getTime() - Date.now().getTime()));
		return Promise.NOISE;
	}
	
	public function unset(id:String):Promise<Noise> {
		switch timers[id] {
			case null:
			case timer:
				timer.stop();
				timers.remove(id);
		}
		return Promise.NOISE;
	}
	
	public function spawnWorker(s):LocalWorker<Payload> {
		final worker = new LocalWorker(this, s);
		workers.push(worker);
		while(pending.length > 0) worker.run(pending.pop());
		return worker;
	}
	
	inline function removeWorker(w) {
		workers.remove(w);
	}
	
	function run(task:Task<Payload>) {
		switch nextWorker() {
			case null: pending.push(task);
			case worker: worker.run(task).eager();
		}
	}
	
	function nextWorker() {
		return switch workers.length {
			case 0: null;
			case len: workers[Std.random(len)];
		}
	}
}