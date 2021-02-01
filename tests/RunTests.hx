package ;

import tink.testrunner.*;
import tink.unit.*;
import why.scheduler.*;
import why.scheduler.redis.*;
import why.scheduler.local.*;
import tink.Chunk;

using DateTools;
using tink.CoreApi;

class RunTests {

	static function main() {
		Runner.run(TestBatch.make([
			{
				final scheduler = new LocalScheduler<MyPayload>();
				final worker = scheduler.spawnWorker();
				new Test(scheduler, worker);
			},
			{
				final key = 'why-scheduler';
				final redis = new Ioredis();
				final scheduler = new RedisScheduler<MyPayload>(Instance(redis), key, serialize);
				final worker = new RedisWorker<MyPayload>(Instance(redis), key, unserialize, {concurrency: 5});
				new Test(scheduler, worker);
			},
		])).handle(Runner.exit);
	}
	
	static function serialize(payload:MyPayload):Chunk
		return tink.Json.stringify(payload);
	
	static function unserialize(chunk:Chunk):Outcome<MyPayload, Error>
		return tink.Json.parse((chunk:MyPayload));
		
}

@:asserts
@:timeout(30000)
class Test {
	static inline final INTERVAL = 200;
	static inline final ROUNDS = 3;
	static inline final TASKS_PER_ROUND = 6;
	static inline final CONCURRENCY = 5;
	static inline final PROCESS_TIME = 250;
	
	final scheduler:Scheduler<MyPayload>;
	final worker:Worker<MyPayload>;
	
	public function new(scheduler, worker) {
		this.scheduler = scheduler;
		this.worker = worker;
	}
	
	public function test() {
		
		final logs = [];
		final process = (filter, task:Task<MyPayload>) -> {
			logs.push({date: Date.now(), filter: filter, task: task});
			Future.delay(PROCESS_TIME, Noise);
		}
		
		for(i in 0...ROUNDS) {
			for(j in 0...TASKS_PER_ROUND) {
				scheduler.set({id: 'task-$i-$j', payload: {i: i, j: j}, at: Date.now().delta((i+1) * INTERVAL)}).eager();
			}
		}
		
		function extract(v:String) {
			final regex = ~/task-(\d)-(\d)/;
			if(regex.match(v)) {
				return Std.parseInt(regex.matched(1));
			} else {
				return -1;
			}
		}
		
		function subscribe(filter) {
			return worker.subscribe(filter, process.bind(filter));
		}
		
		final w1 = subscribe(task -> extract(task.id) % 2 == 0);
		final w2 = subscribe(task -> extract(task.id) % 2 == 1);
		
		haxe.Timer.delay(w1, PROCESS_TIME);
		haxe.Timer.delay(subscribe.bind(task -> extract(task.id) % 2 == 0), INTERVAL * 2 + PROCESS_TIME);
		haxe.Timer.delay(function() {
			asserts.assert(logs.length == ROUNDS * TASKS_PER_ROUND);
			for(log in logs) {
				asserts.assert(log.filter(log.task)); // make sure task is processed by the correct subscriber
				asserts.assert(log.task.payload.i >= 0); // make sure data is intact
				asserts.assert(log.task.payload.j >= 0); // make sure data is intact
			}
			asserts.done();
		}, ROUNDS * INTERVAL + PROCESS_TIME * TASKS_PER_ROUND);
		
		return asserts;
	}
}


typedef MyPayload = {
	final i:Int;
	final j:Int;
}