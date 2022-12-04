package com.mozvil.time;

import java.util.Iterator;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.curator5.com.google.common.annotations.VisibleForTesting;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

public class CustomTimeEvictor implements Evictor<Object, TimeWindow> {

	private static final long serialVersionUID = 3776334235576172942L;
	
	private long windowSize;
	private boolean doEvictAfter;
	private String flagEventId;
	
	public CustomTimeEvictor(long windowSize) {
		this.windowSize = windowSize;
		this.doEvictAfter = false;
	}
	
	public CustomTimeEvictor(long windowSize, boolean doEvictAfter) {
		this.windowSize = windowSize;
		this.doEvictAfter = doEvictAfter;
	}
	
	public CustomTimeEvictor(long windowSize, boolean doEvictAfter, String flagEventId) {
		this.windowSize = windowSize;
		this.doEvictAfter = doEvictAfter;
		this.flagEventId = flagEventId;
	}
	
	
	// evictBefor和evictAfter两个方法会在窗口计算触发的前后被调用 可以在这里对数据进行清除
	@Override
	public void evictBefore(Iterable<TimestampedValue<Object>> elements, int size, TimeWindow window, EvictorContext ctx) {
		if(!doEvictAfter) {
			myEvict(elements, size, ctx);
		}
	}
	
	@Override
	public void evictAfter(Iterable<TimestampedValue<Object>> elements, int size, TimeWindow window, EvictorContext ctx) {
		if(doEvictAfter) {
			myEvict(elements, size, ctx);
		}
	}
	
	@SuppressWarnings("unchecked")
	private void myEvict(Iterable<TimestampedValue<Object>> elements, int size, EvictorContext ctx) {
		long currentTime = getMaxTimestamp(elements);
		long evictCutoff = currentTime - size;
		// 原有的逻辑是将数据时间小于窗口起始点的数据全部移除
		
		for(Iterator<TimestampedValue<Object>> iterator = elements.iterator(); iterator.hasNext();) {
			TimestampedValue<Object> record = iterator.next();
			// 取出数据转成Tuple2<EventBean2, Integer>
			Tuple2<EventBean2, Integer> data = (Tuple2<EventBean2, Integer>) record.getValue();
			// 现增加一个特殊逻辑：将数据中eventId为e0x的数据也一并移除(不参与计算)
			if(record.getTimestamp() <= evictCutoff || data.f0.getEventId().equals(flagEventId)) {
				iterator.remove();
			}
		}
	}
	
	@SuppressWarnings("unused")
	private boolean hasTimestamp(Iterable<TimestampedValue<Object>> elements) {
		Iterator<TimestampedValue<Object>> it = elements.iterator();
		if(it.hasNext()) {
			return it.next().hasTimestamp();
		}
		return false;
	}
	
	private long getMaxTimestamp(Iterable<TimestampedValue<Object>> elements) {
        long currentTime = Long.MIN_VALUE;
        for (Iterator<TimestampedValue<Object>> iterator = elements.iterator(); iterator.hasNext();) {
            TimestampedValue<Object> record = iterator.next();
            currentTime = Math.max(currentTime, record.getTimestamp());
        }
        return currentTime;
    }
	
	@Override
	public String toString() {
		return "TimeEvictor(" + windowSize + ")";
	}
	
	@VisibleForTesting
	public long getWindowSize() {
		return windowSize;
	}
	
	public static <W extends Window> CustomTimeEvictor of(Time windowSize) {
		return new CustomTimeEvictor(windowSize.toMilliseconds());
	}
	
	public static <W extends Window> CustomTimeEvictor of(Time windowSize, boolean doEvictAfter) {
		return new CustomTimeEvictor(windowSize.toMilliseconds(), doEvictAfter);
	}

}
