package taskmgr

import (
	"reflect"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dodynamiccast/log"
	"github.com/dodynamiccast/pool"
)

const (
	FLAG_RUNNING    = 1
	FLAG_BEGIN_EXIT = 2
	FLAG_EXITED     = 3
	FLAG_PANIC      = 4
)

type HTaskNode struct {
	args     []reflect.Value
	callFunc *reflect.Value
	resChan  chan int

	retValue []interface{}

	hashKey uint64
	isOrder bool

	next *HTaskNode
}

type TaskCtx struct {
	taskId  int //任务类型
	hashKey uint64
	isOrder bool
	f       interface{}
	parent  *TaskCtx //父调用
}

func (tc *TaskCtx) Print() {
	for tc != nil {
		log.Printf("tc %+v", tc)
		tc = tc.parent
	}
}

type TaskHashNode struct {
	sync.RWMutex
	isRunning bool //是否在执行中，或者在taskChan
	//isDeleted  bool       //是否已删除
	fQueueHead *HTaskNode //等待队列头部，头取
	fQueueTail *HTaskNode //等待队列尾部，尾插
}

//任务等待
type TaskWaiter interface {
	Add(delta int)
	Done()
}

type DeaultWaiter struct {
}

func (w *DeaultWaiter) Add(delta int) {
}

func (w *DeaultWaiter) Done() {
}

var Waiter TaskWaiter = &DeaultWaiter{}

type TaskMgr struct {
	WaitTimeOut uint64

	taskChan     chan *HTaskNode
	exitChan     chan int
	maxThreadNum int32
	threadNum    int32
	minThreadNum int32
	idleNum      int32
	nodePool     pool.ObjPool
	threadLock   sync.Mutex
	isFixNum     bool
	taskId       int

	hashNodePool pool.ObjPool
	hashLock     sync.RWMutex
	hashMap      map[uint64]*TaskHashNode //执行状态以及等待队列
}

func (tm *TaskMgr) onFunPanic(task *HTaskNode) {
	log.StLogger.WriteLog(log.PanicLevel, 1, "get panic :", task.args)
	if task.callFunc.Kind() != reflect.Func {
		log.StLogger.WriteLog(log.PanicLevel, 1, "get panic :", "func type error")
		return
	}
	f := runtime.FuncForPC(task.callFunc.Pointer())
	file, l := f.FileLine(f.Entry())
	log.StLogger.WriteLog(log.PanicLevel, 1, "get panic :", file, ":", l, " ", f.Name())
}

func (tm *TaskMgr) run() {
	exitFlag := FLAG_RUNNING
	var task *HTaskNode
	task = nil
	defer func() {
		atomic.AddInt32(&tm.threadNum, -1)
		if exitFlag == FLAG_BEGIN_EXIT {
			return
		} else {
			log.StLogger.WriteLog(log.PanicLevel, 1, string(debug.Stack()))
			if task != nil {
				task.resChan <- 0
				tm.onFunPanic(task)
			}
			if err := recover(); err != nil {
				log.StLogger.WriteLog(log.PanicLevel, 1, err)
			}
		}
		if task != nil {
			task = tm.onTaskDone(task)
			if task != nil {
				tm.taskChan <- task
			}
		}
	}()

	tm.threadLock.Lock()
	atomic.AddInt32(&tm.threadNum, 1)
	if tm.threadNum > tm.maxThreadNum {
		exitFlag = FLAG_BEGIN_EXIT
		tm.threadLock.Unlock()
		return
	}
	tm.threadLock.Unlock()

for_run_routine:
	for {
		//var task *HTaskNode
		if task == nil {
			select {
			case task = <-tm.taskChan:
			case <-tm.exitChan:
				exitFlag = FLAG_BEGIN_EXIT
				break for_run_routine
			}
		}
		(*(task.callFunc)).Call(task.args)

		task.resChan <- 0
		task = tm.onTaskDone(task)
	}
}

//协程数量超过min_num，len(task)小于1/4容量，或者task队列为空，触发回收
func (tm *TaskMgr) recycleRoutine() {
	for {
		if tm.threadNum > tm.minThreadNum && (len(tm.taskChan) < (cap(tm.taskChan))/4 || len(tm.taskChan) == 0) {
			var recycleNum = cap(tm.exitChan)
			if (int)(tm.threadNum-tm.minThreadNum) < (int)(cap(tm.exitChan)) {
				recycleNum = (int)(tm.threadNum - tm.minThreadNum)
			}
			for i := 0; i < recycleNum; i++ {
				tm.exitChan <- 1
			}
		}
		time.Sleep(time.Millisecond * 500)
	}
}

func (tm *TaskMgr) Start(maxThreadNum int, minThreadNum int, chanLen int, isFixNum bool) {
	tm.nodePool.InitRv(reflect.TypeOf((*HTaskNode)(nil)), minThreadNum)
	//tm.hashNodePool.InitRv(reflect.TypeOf((*TaskHashNode)(nil)), 1000)
	tm.minThreadNum = (int32)(minThreadNum)
	tm.maxThreadNum = (int32)(maxThreadNum)
	tm.taskChan = make(chan *HTaskNode, chanLen)
	tm.exitChan = make(chan int, minThreadNum)
	tm.isFixNum = isFixNum
	tm.hashMap = make(map[uint64]*TaskHashNode)
	tm.hashNodePool.InitRv(reflect.TypeOf((*TaskHashNode)(nil)), minThreadNum)

	if tm.isFixNum == false {
		go tm.recycleRoutine()
	}
	//默认5s过期
	tm.WaitTimeOut = 5 * 1000
	/*	for i := (int32)(0); i < tm.maxThreadNum; i++ {
		go tm.run()
	}*/
}

func (tm *TaskMgr) recycleNode(node *HTaskNode) {
	node.args = nil
	node.callFunc = nil
	node.retValue = nil
	node.isOrder = false
	tm.nodePool.Free(node)
}

func (tm *TaskMgr) recycleHashNode(node *TaskHashNode) {
	node.isRunning = false
	//node.fQueue = make(list.List)
	tm.hashNodePool.Free(node)
}

func (tm *TaskMgr) onTaskDone(task *HTaskNode) *HTaskNode {
	defer tm.recycleNode(task)
	defer Waiter.Done()
	if task.isOrder == false {
		return nil
	}
	tm.hashLock.Lock()
	hashNode, ok := tm.hashMap[task.hashKey]
	if !ok || hashNode == nil {
		panic("no node find error")
	}
	//如果等待队列为空，则删掉hashNode
	//否则取出头部元素，放入执行队列
	if hashNode.fQueueHead == nil {
		delete(tm.hashMap, task.hashKey)
		tm.recycleHashNode(hashNode)
		tm.hashLock.Unlock()
		//fmt.Printf("clean queue\n")
	} else {
		hashNode.isRunning = true
		nTask := hashNode.fQueueHead
		hashNode.fQueueHead = nTask.next
		if hashNode.fQueueHead == nil {
			hashNode.fQueueTail = nil
		}
		tm.hashLock.Unlock()
		select {
		case tm.taskChan <- nTask:
		default:
			return nTask
		}
		//fmt.Printf("add wait task\n")
	}
	return nil
}

func (tm *TaskMgr) addTask(task *HTaskNode) {
	//先上全局读锁,检查是否有节点
	tm.hashLock.RLock()
	hashNode, ok := tm.hashMap[task.hashKey]
	if !ok {
		//不存在，解读锁，换全局写锁
		tm.hashLock.RUnlock()
		tm.hashLock.Lock()
		//进入写锁后，需要再次检查节点是否存在
		if hashNode, ok = tm.hashMap[task.hashKey]; !ok {
			hashNode = tm.hashNodePool.Alloc().(*TaskHashNode)
			//hashNode.Lock()
			hashNode.fQueueTail = nil
			hashNode.fQueueHead = nil
			hashNode.isRunning = true
			tm.hashMap[task.hashKey] = hashNode
			//hashNode.Unlock()
			tm.hashLock.Unlock()
			//fmt.Printf("into queue %+v\n", task)
			tm.taskChan <- task
		} else {
			//存在节点，把任务挂到队尾
			//此处上节点锁
			hashNode.Lock()
			task.next = nil
			hashNode.isRunning = true
			if hashNode.fQueueTail != nil {
				hashNode.fQueueTail.next = task
				hashNode.fQueueTail = task
			} else {
				hashNode.fQueueTail = task
				hashNode.fQueueHead = hashNode.fQueueTail
				task.next = nil
			}
			hashNode.Unlock()
			tm.hashLock.Unlock()
		}
	} else {
		//存在节点，把任务挂到队尾
		//此处上节点锁
		hashNode.Lock()
		//fmt.Printf("exist add task pre %+v\n", task)
		if hashNode.isRunning == false && hashNode.fQueueHead == nil {
			hashNode.isRunning = true
			tm.taskChan <- task
		} else {
			hashNode.isRunning = true
			task.next = nil
			if hashNode.fQueueTail != nil {
				hashNode.fQueueTail.next = task
				hashNode.fQueueTail = task
			} else {
				hashNode.fQueueTail = task
				hashNode.fQueueHead = hashNode.fQueueTail
				task.next = nil
			}
		}
		hashNode.Unlock()
		tm.hashLock.RUnlock()
	}
}

func (tm *TaskMgr) addFunc(task *HTaskNode) {
	if task.isOrder == false {
		tm.taskChan <- task
		return
	}
	tm.addTask(task)
	return
}

func checkFunc(f *reflect.Value) {
	if f.Kind() != reflect.Func {
		panic("get invalid func ptr")
	}
}

func (tm *TaskMgr) callGo(isOrder bool, hashKey uint64, cf interface{}, args ...interface{}) error {
	//fmt.Printf("and func %+v\n", hashKey)
	task := tm.nodePool.Alloc().(*HTaskNode)
	task.args = make([]reflect.Value, len(args))
	for i := 0; i < len(args); i++ {
		task.args[i] = reflect.ValueOf(args[i])
	}

	callFunc := reflect.ValueOf(cf)
	checkFunc(&callFunc)
	task.callFunc = &callFunc
	task.hashKey = hashKey
	task.isOrder = isOrder
	Waiter.Add(1)

	if (tm.threadNum < tm.maxThreadNum && len(tm.taskChan) >= cap(tm.taskChan)) || tm.threadNum < tm.minThreadNum {
		go tm.run()
	}
	task.resChan = make(chan int, 1)
	if !isOrder {
		tm.taskChan <- task
	} else {
		tm.addFunc(task)
	}
	return nil
}

func (tm *TaskMgr) callGroup(isOrder bool, hashKey uint64, cf interface{}, args ...interface{}) chan int {
	//fmt.Printf("and func %+v\n", hashKey)
	task := tm.nodePool.Alloc().(*HTaskNode)
	task.args = make([]reflect.Value, len(args))
	for i := 0; i < len(args); i++ {
		task.args[i] = reflect.ValueOf(args[i])
	}

	callFunc := reflect.ValueOf(cf)
	checkFunc(&callFunc)
	task.callFunc = &callFunc
	task.hashKey = hashKey
	task.isOrder = isOrder
	Waiter.Add(1)

	if (tm.threadNum < tm.maxThreadNum && len(tm.taskChan) >= cap(tm.taskChan)) || tm.threadNum < tm.minThreadNum {
		go tm.run()
	}
	task.resChan = make(chan int, 1)
	if !isOrder {
		tm.taskChan <- task
	} else {
		tm.addFunc(task)
	}
	return task.resChan
}

func (tm *TaskMgr) call(isOrder bool, hashKey uint64, cf interface{}, args ...interface{}) {
	task := tm.nodePool.Alloc().(*HTaskNode)
	task.args = make([]reflect.Value, len(args))
	for i := 0; i < len(args); i++ {
		task.args[i] = reflect.ValueOf(args[i])
	}

	callFunc := reflect.ValueOf(cf)
	if callFunc.Kind() != reflect.Func {
	}
	task.callFunc = &callFunc
	checkFunc(&callFunc)
	task.hashKey = hashKey
	task.isOrder = isOrder
	Waiter.Add(1)

	if (tm.threadNum < tm.maxThreadNum && len(tm.taskChan) >= cap(tm.taskChan)) || tm.threadNum < tm.minThreadNum {
		go tm.run()
	}
	task.resChan = make(chan int, 1)

	if !isOrder {
		tm.taskChan <- task
	} else {
		tm.addFunc(task)
	}
	<-task.resChan

	//return result, retVal
}

//同步调用，需要维护调用链关系
func (tm *TaskMgr) callCtx(pctx *TaskCtx, isOrder bool, hash uint64, cf interface{}, args ...interface{}) {
	var tc TaskCtx
	tc.hashKey = hash
	tc.taskId = tm.taskId
	tc.parent = pctx
	tc.isOrder = isOrder
	tc.f = cf
	var rargs = make([]interface{}, 0, len(args)+1)
	rargs = append(rargs, &tc)
	rargs = append(rargs, args...)

	tm.call(isOrder, hash, cf, rargs...)
}

//异步调用，另开调用链
func (tm *TaskMgr) callCtxGo(isOrder bool, hash uint64, cf interface{}, args ...interface{}) {
	var tc TaskCtx
	tc.hashKey = hash
	tc.taskId = tm.taskId
	tc.parent = nil
	tc.isOrder = isOrder
	tc.f = cf
	var rargs = make([]interface{}, 0, len(args)+1)
	rargs = append(rargs, &tc)
	rargs = append(rargs, args...)
	tm.callGo(isOrder, hash, cf, rargs...)
}

//同步调用，需要维护调用链关系
func (tm *TaskMgr) callCtxGroup(pctx *TaskCtx, isOrder bool, hash uint64, cf interface{}, args ...interface{}) chan int {
	var tc TaskCtx
	tc.hashKey = hash
	tc.taskId = tm.taskId
	tc.parent = pctx
	tc.isOrder = isOrder
	tc.f = cf
	var rargs = make([]interface{}, 0, len(args)+1)
	rargs = append(rargs, &tc)
	rargs = append(rargs, args...)
	return tm.callGroup(isOrder, hash, cf, rargs...)
}

/*func (tm *TaskMgr) CallS(cf interface{}, args ...interface{}) (int, []interface{}) {
	return tm.Call(cf, args...)
}*/
