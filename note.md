support filter
- poll all "ready" items in ordered set with ZRANGE BYSCORE [0, Date.now().getTime()]
- filter tasks with user-provided func and then ZREM it(/them if concurrency allowed), invoke subscriber function if successful
- bonus: remove expired tasks with TTL