package why.scheduler.web;

import why.scheduler.base.PollWorker;
import why.scheduler.Task;

using tink.CoreApi;

typedef WebWorkerOptions = {
	?interval:Int,
}

class WebWorker<Payload> extends PollWorker<Payload> {
	public function new(remote, ?options:WebWorkerOptions) {
		super(new WebPoller(remote), switch options {
			case null: null;
			case {interval: v}: v;
		});
	}
}