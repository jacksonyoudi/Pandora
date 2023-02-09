package org.youdi.ch05


/**
 * 算子链条
 * 1. 上下游实例之间是oneToOne数据传输 forward
 * 2. 上下游算子并行度相同
 * 3. 上下游算子属于相同的slotSharingGroup 槽位共享组
 *
 * setParalllelism
 * slotSharingGroup
 * disableChaning
 * startNewChain
 *
 *
 * 分区partition算子
 * 1. global 全部发往到第一个task
 * 2. broadcast 广播
 * 3. forward  oneToOne
 * 4. shuffle  洗牌
 * 5. rebalance round-robin
 * 6. recale 本地轮流分配
 * 7 partitionCustom
 *
 * 默认情况下是使用rebalance
 * 
 */
object Demo {

}