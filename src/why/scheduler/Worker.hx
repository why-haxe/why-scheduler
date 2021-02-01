package why.scheduler;

using tink.CoreApi;

interface Worker<Payload> {
	function subscribe(filter:Filterer<Payload>, subscriber:Subscriber<Payload>):CallbackLink;
	function destroy():Future<Noise>;
}

typedef Filterer<Payload> = Task<Payload>->Bool;
typedef Subscriber<Payload> = Task<Payload>->Future<Noise>;