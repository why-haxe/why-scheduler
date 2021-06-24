package why.scheduler.redis;

class RedisBase {
	final redis:ioredis.Redis;
	final zkey:String;
	final hkey:String;
	
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