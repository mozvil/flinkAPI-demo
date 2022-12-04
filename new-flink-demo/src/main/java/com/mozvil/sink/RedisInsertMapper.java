package com.mozvil.sink;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import com.alibaba.fastjson.JSON;
import com.mozvil.demo.Event;

public class RedisInsertMapper implements RedisMapper<Event> {

	private static final long serialVersionUID = -5721477294322375023L;

	@Override
	public RedisCommandDescription getCommandDescription() {
		// 往redis中放list结构的数据(RPUSH方式为向后插入)
		// 放入list结构数据时不需要第二个参数(additionalKey) 这个Key由下面的getKeyFromData方法指定
		// 放入set结构数据时传入第二个参数 此参数将作为整个set数据的大key 此时getKeyFromData方法指定set内部每个KV里的key值
		return new RedisCommandDescription(RedisCommand.RPUSH);
		//return new RedisCommandDescription(RedisCommand.HSET, "eventlogs");
	}

	// 此方法返回key: 
	//    #1 如果value不是KV结构的(HSET)数据，这里返回的是redis数据中的大Key
	//    #2 如果value中是KV结构的数据，则这里返回的是HSET中的key(不能重复)
	@Override
	public String getKeyFromData(Event data) {
		//return data.getEventId() + "-" + data.getTimestamp();
		return "eventlogs";
	}

	@Override
	public String getValueFromData(Event data) {
		// 获取JSON字符串
		return JSON.toJSONString(data);
	}

}
