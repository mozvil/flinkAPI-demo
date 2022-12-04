package com.mozvil.time;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class CustomEventTimeTrigger extends Trigger<Tuple2<EventBean2, Integer>, TimeWindow> {

	private static final long serialVersionUID = -3069328159604340567L;

	private CustomEventTimeTrigger() {}

	/**
	 * 检查数据时间是否达到窗口关闭的条件
	 */
    @Override
    public TriggerResult onElement(
    		Tuple2<EventBean2, Integer> element, long timestamp, TimeWindow window, TriggerContext ctx)
            throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            return TriggerResult.FIRE;
        } else {
        	// 重新注册一个定时器，时长为窗口的最大长度 定时器到点就会触发onEventTime()方法
            ctx.registerEventTimeTimer(window.maxTimestamp());
            // 如果数据中有eventId为e0x的 就触发一下 事件时间到达仍然还会触发
            if(element.f0.getEventId().contains("e0x")) {
            	return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }
    }

    /**
     * 检查事件定时器时间是否达到窗口关闭的条件
     */
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    /**
     * 检查处理定时器时间是否达到窗口关闭的条件
     */
    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
            throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) {
        // only register a timer if the watermark is not yet past the end of the merged window
        // this is in line with the logic in onElement(). If the watermark is past the end of
        // the window onElement() will fire and setting a timer here would fire the window twice.
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
    }

    @Override
    public String toString() {
        return "EventTimeTrigger()";
    }

    /**
     * Creates an event-time trigger that fires once the watermark passes the end of the window.
     *
     * <p>Once the trigger fires all elements are discarded. Elements that arrive late immediately
     * trigger window evaluation with just this one element.
     */
    public static CustomEventTimeTrigger create() {
        return new CustomEventTimeTrigger();
    }

}
