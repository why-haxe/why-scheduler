package why.scheduler;

using tink.CoreApi;

typedef Task<Payload> = {
	final id:String;
	final payload:Payload;
	final window:Window;
}

abstract Window(Pair<Date, Null<Date>>) from Pair<Date, Null<Date>> to Pair<Date, Null<Date>> {
	public var from(get, never):Date;
	public var to(get, never):Null<Date>;
	
	@:from public static inline function fromDate(date:Date):Window
		return new Pair(date, null);
	
	@:from public static inline function fromObject(obj:{from:Date, to:Date}):Window
		return new Pair(obj.from, obj.to);
	
	public inline function expired(now:Date)
		return to != null && to.getTime() > now.getTime();
	
	inline function get_from()
		return this.a;
	
	inline function get_to()
		return this.b;
}