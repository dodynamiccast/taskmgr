
任务管理器组件，用于协程数量控制，并发排队等逻辑。

用法：
var htask taskmgr.HTaskManager
//HTaskManager.Start(MaxGroutineNum, MinGroutineNum)，MinGroutineNum同时也是内部channel大小限制
htask.Start(30000, 100) 
//HTaskManager.Call(hashKey, func, ...args)，同一个taskmanager，对同一个hashkey的调用，会保证按照顺序调用。同步等待func结束
htask.Call(uint64(1), func(i int){
  fmt.Printf("input %d\n", i)
}, 10)
//htask.Call异步版本
htask.CallGo(uint64(1), func(i int) {
  fmt.Printf("input %d\n", i)
}, 12)

性能比较（同等条件虚拟机）：
CallGo接口：63w qps
go关键字： 60w qps
