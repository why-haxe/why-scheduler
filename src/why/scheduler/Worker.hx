package why.scheduler;

using tink.CoreApi;

interface Worker<Payload> {
	function subscribe(subscriber:Subscriber<Payload>, ?options:SubscribeOptions<Payload>):CallbackLink;
	function destroy():Future<Noise>;
}

typedef SubscribeOptions<Payload> = {
	final ?filter:Filterer<Payload>;
	final ?semaphore:why.Semaphore<Noise>;
}

@:callable
abstract Filterer<Payload>(Task<Payload>->Bool) from Task<Payload>->Bool to Task<Payload>->Bool {
	public static function allow<T>(v:T) {
		return true;
	}
}
typedef Subscriber<Payload> = Task<Payload>->Future<Noise>;

@:forward
abstract Subscription<Payload>(SubscriptionObject<Payload>) from SubscriptionObject<Payload> to SubscriptionObject<Payload> {
	public inline function new(subscriber:Subscriber<Payload>, ?options:SubscribeOptions<Payload>) {
		this = {
			subscriber: subscriber,
			filter: switch options {
				case null | {filter: null}: Filterer.allow;
				case {filter: f}: f;
			},
			semaphore: switch options {
				case null | {semaphore: null}: new why.concurrency.semaphore.Unlimited();
				case {semaphore: s}: s;
			},
		}
	}
}
typedef SubscriptionObject<Payload> = {
	final subscriber:Subscriber<Payload>;
	final filter:Filterer<Payload>;
	final semaphore:why.Semaphore<Noise>;
}