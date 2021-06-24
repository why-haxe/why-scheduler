package why.scheduler.redis;

class RedisDriver {
	public final redis:ioredis.Redis;
	public final zkey:String;
	public final hkey:String;
	
	public function new(redisKind:RedisKind, key:String) {
		this.redis = switch redisKind {
			case Instance(inst): inst;
			case Options(opt): new Ioredis(opt);
		}
		this.zkey = key + '-time';
		this.hkey = key + '-payload';
	}
}

enum RedisKind {
	Instance(redis:ioredis.Redis);
	Options(options:ioredis.RedisOptions);
}