package why.scheduler.base;

import why.scheduler.Task;

using tink.CoreApi;

interface Poller<Payload> {
	/**
	 * List tasks that are ready to be handled
	 * @return Promise<Array<Task<Payload>>>
	 */
	 function list(now:Date):Promise<Array<Task<Payload>>>;
	 
	 /**
	  * Get a task atomically 
	  * @param id 
	  * @return Promise<Bool> true if successfully obtained ownership
	  */
	 function get(id:String):Promise<Bool>;
}