package why.scheduler.local;

import haxe.Timer;
import why.scheduler.Scheduler;

using tink.CoreApi;

@:access(why.scheduler.local)
class LocalScheduler<Payload> implements Scheduler<Payload> {
	
	final timers:Map<String, Timer> = [];
	final workers:Array<LocalWorker<Payload>> = [];
	final pending:Array<Task<Payload>> = []; // tasks with timer triggered but no available workers yet
	
	var index:Int = 0;
	
	public function new() {}
	
	public function set(task:Task<Payload>):Promise<Noise> {
		switch timers[task.id] {
			case null:
			case timer: timer.stop();
		}
		
		timers[task.id] = Timer.delay(trigger.bind(task), Std.int(task.window.from.getTime() - Date.now().getTime()));
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
	
	public function spawnWorker():LocalWorker<Payload> {
		final worker = new LocalWorker(this);
		workers.push(worker);
		return worker;
	}
	
	inline function removeWorker(w) {
		workers.remove(w);
	}
	
	// triggers a task when its timer is expired
	function trigger(task:Task<Payload>) {
		if(!task.window.expired(Date.now())) {
			for(worker in workers) {
				switch worker.getSubscriber(task) {
					case null: // continue;
					case s:
						s(task).eager();
						return;
				}
			}
			
			// push to pending if no worker is available to handle this task
			pending.push(task);
		}
		
	}
	
	function runPending(worker:LocalWorker<Payload>) {
		final tasks = pending.copy();
		pending.resize(0);
		final now = Date.now();
		for(task in tasks)
			if(!task.window.expired(now))
				switch worker.getSubscriber(task) {
					case null: pending.push(task);
					case s: s(task).eager();
				}
	}
	
}