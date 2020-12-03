package why.scheduler;

using tink.CoreApi;

interface Worker<Payload> {
	function subscribe(cb:Subscriber<Payload>):CallbackLink;
}

typedef Subscriber<Payload> = Task<Payload>->Future<Noise>;