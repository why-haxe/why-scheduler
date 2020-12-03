package why.scheduler;

typedef Task<Payload> = {
	final id:String;
	final payload:Payload;
	final at:Date;
}