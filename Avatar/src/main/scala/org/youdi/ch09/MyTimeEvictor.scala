package org.youdi.ch09

import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue

import java.{lang, util}

class MyTimeEvictor(var windowSize: Long, var doEvictAfter: Boolean) extends Evictor[EventLog, TimeWindow] {
  def this(windowSize: Long) {
    this(windowSize, false)
  }

  /**
   * 窗口触发前，调用
   */
  override def evictBefore(elements: lang.Iterable[TimestampedValue[EventLog]], size: Int, window: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {
    if (!doEvictAfter) {
      evict(elements, size, evictorContext)
    }
  }

  /**
   * 窗口触发后，调用
   *
   * @param elements
   * @param size
   * @param window
   * @param evictorContext
   */
  override def evictAfter(elements: lang.Iterable[TimestampedValue[EventLog]], size: Int, window: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {
    if (doEvictAfter) {
      evict(elements, size, evictorContext)
    }
  }

  private def hasTimestamp(elements: lang.Iterable[TimestampedValue[EventLog]]): Boolean = {
    val it: util.Iterator[TimestampedValue[EventLog]] = elements.iterator()
    if (it.hasNext) {
      return it.next().hasTimestamp
    }
    false

  }

  private def getMaxTimestamp(elements: lang.Iterable[TimestampedValue[EventLog]]): Long = {
    var value: Long = Long.MinValue
    elements.forEach(
      ts => {
        value = math.max(ts.getTimestamp, value)
      }
    )
    value
  }

  private def evict(elements: lang.Iterable[TimestampedValue[EventLog]], size: Int, evictorContext: Evictor.EvictorContext): Unit = {
    if (!hasTimestamp(elements)) {
      return
    }
    val currentTime: Long = getMaxTimestamp(elements)
    val evictCutoff: Long = currentTime - windowSize

    elements.forEach(
      r => {
        val value: EventLog = r.getValue
        if (r.getTimestamp <= evictCutoff || value.page.equals("test")) {
          // 移除这条记录
        }
      }
    )
  }
}
