package why.scheduler;

using tink.CoreApi;

class Quota {
	public final max:Int;
	public var vacancy(default, null):Int;
	
	public final available:Signal<Noise>;
	
	final availableTrigger:SignalTrigger<Noise>;
	
	public function new(max) {
		this.max = max;
		this.vacancy = max;
		
		available = availableTrigger = Signal.trigger();
	}
	
	public function acquire():Option<CallbackLink> {
		return
			if(vacancy > 0) {
				vacancy--;
				Some(release);
			} else {
				None;
			}
	}

	function release() {
		if(vacancy++ == 0) {
			Callback.defer(availableTrigger.trigger.bind(Noise));
		}
	}
}