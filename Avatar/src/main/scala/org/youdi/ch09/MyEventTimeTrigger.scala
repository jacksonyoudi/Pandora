package org.youdi.ch09

import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class MyEventTimeTrigger extends Trigger[EventLog, TimeWindow] {
  def apply(): MyEventTimeTrigger = {
    new MyEventTimeTrigger
  }

  /**
   * 来一条数据时，需要检查watermark是否已经越过窗口结束点需要触发
   *
   * @param element
   * @param timestamp
   * @param window
   * @param ctx
   * @return
   */
  override def onElement(element: EventLog, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    // 如果窗口结束点 <= 当前的watermark
    if (window.maxTimestamp <= ctx.getCurrentWatermark) {
      TriggerResult.FIRE
    } else {
      // 注册定时器，定时器的触发时间为： 窗口的结束点时间
      ctx.registerEventTimeTimer(window.maxTimestamp())

      // 判断，当前数据的用户行为事件id是否等于e0x，如是，则触发
      if ("fire".equals(element.page)) {
        return TriggerResult.FIRE
      }
      TriggerResult.CONTINUE

    }
  }

  /**
   * 当处理时间定时器的触发时间（窗口的结束点时间）到达了，检查是否满足触发条件
   */
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  /**
   * 当事件时间定时器的触发时间（窗口的结束点时间）到达了，检查是否满足触发条件
   * 下面的方法，是定时器在调用
   */
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    if (time == window.maxTimestamp()) {
      TriggerResult.FIRE
    } else {
      TriggerResult.CONTINUE
    }
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}
