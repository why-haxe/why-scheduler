package why.scheduler;

using tink.CoreApi;

interface Scheduler<Payload> {
	function set(task:Task<Payload>):Promise<Noise>;
	function unset(id:String):Promise<Noise>;
}