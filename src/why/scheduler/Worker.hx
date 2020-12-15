package why.scheduler;

using tink.CoreApi;

interface Worker<Payload> {
	function destroy():Future<Noise>;
}

typedef Subscriber<Payload> = Task<Payload>->Future<Noise>;