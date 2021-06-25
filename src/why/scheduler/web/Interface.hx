package why.scheduler.web;

using tink.CoreApi;

typedef Interface<Payload> = {
	@:get('/$id')
	function get(id:String):Promise<Bool>;
	
	@:get('/')
	@:params(date in query)
	function list(date:Date):Promise<Array<Task<Payload>>>;
	
	@:post
	@:params(task = body)
	@:consumes('application/json')
	function set(task:Task<Payload>):Promise<Noise>;
	
	@:delete('/$id')
	function unset(id:String):Promise<Noise>;
}