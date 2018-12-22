package taskmgr

import (
	"reflect"
	"sync"
)

//hash任务队列
type HTaskManager struct {
	hTasks  []TaskMgr
	divNum  uint64
	incSeed uint64

	taskId int
}

var taskIdSeed = 10000
var taskIdLock sync.Mutex

func (h *HTaskManager) Start(threadNum int, chLen int) {
	/*if runtime.NumCPU()/4 > 0 {
		h.divNum = uint64(runtime.NumCPU() / 4)
	} else {
		h.divNum = 6
	}*/
	h.divNum = 4
	h.incSeed = 0
	h.hTasks = make([]TaskMgr, h.divNum)
	taskIdLock.Lock()
	taskIdSeed++
	h.taskId = taskIdSeed
	taskIdLock.Unlock()
	//fmt.Printf("task mgrs %+v\n", h.divNum)
	//最小协程数设置为channel长度
	//某些场景下，最大协程数无法预测，需要设为非常大
	var minThreadNum = 10
	if chLen > 0 {
		minThreadNum = chLen
	}
	for i := uint64(0); i < h.divNum; i++ {
		h.hTasks[i].Start(threadNum, minThreadNum, chLen, false)
		h.hTasks[i].taskId = h.taskId
	}
}

//同步调用，不需要排队
func (h *HTaskManager) CallR(cf interface{}, args ...interface{}) {
	h.incSeed++
	h.hTasks[h.incSeed%h.divNum].call(false, h.incSeed, cf, args...)
}

//异步调用，不需要排队
func (h *HTaskManager) CallGoR(cf interface{}, args ...interface{}) {
	h.incSeed++
	h.hTasks[h.incSeed%h.divNum].callGo(false, h.incSeed, cf, args...)
}

//同步调用，需要排队
func (h *HTaskManager) Call(hash uint64, cf interface{}, args ...interface{}) {
	h.incSeed++
	h.hTasks[hash%h.divNum].call(true, hash, cf, args...)
}

//异步调用，需要排队
func (h *HTaskManager) CallGo(hash uint64, cf interface{}, args ...interface{}) {
	h.incSeed++
	h.hTasks[hash%h.divNum].callGo(true, hash, cf, args...)
}

func (h *HTaskManager) CallGoW(hash uint64, cf interface{}, args ...interface{}) {
	h.incSeed++
	h.hTasks[hash%h.divNum].callGo(true, hash, cf, args...)
}

func (h *HTaskManager) CallS(hash uint64, cf interface{}, args ...interface{}) {
	h.incSeed++
	h.hTasks[hash%h.divNum].call(true, hash, cf, args...)
}

//内部展开调用
func (h *HTaskManager) callInside(pctx *TaskCtx, isOrder bool, hash uint64, cf interface{}, args ...interface{}) (int, []interface{}) {
	var tc TaskCtx
	tc.hashKey = hash
	tc.taskId = h.taskId
	tc.isOrder = isOrder
	tc.parent = pctx
	tc.f = cf
	var rargs = make([]reflect.Value, len(args)+1)
	rargs[0] = reflect.ValueOf(&tc)
	for i := 0; i < len(args); i++ {
		rargs[i+1] = reflect.ValueOf(args[i])
	}
	ret := reflect.ValueOf(cf).Call(rargs)

	result := 0
	var retValue []interface{}
	for _, v := range ret {
		switch v.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
			if v.IsNil() {
				retValue = append(retValue, nil)
			} else {
				retValue = append(retValue, v.Interface())
			}
		default:
			retValue = append(retValue, v.Interface())
		}
	}
	return result, retValue
}

//上溯同步调用链，如果有同key且taskid一致的任务，则认为本任务需要立即执行，不能投递任务
func (h *HTaskManager) isStuckCall(ctx *TaskCtx, hashKey uint64) bool {
	for ctx != nil {
		if ctx.isOrder == false {
			return false
		}
		if ctx.taskId == h.taskId && ctx.hashKey == hashKey && ctx.isOrder == true {
			return true
		}
		ctx = ctx.parent
	}
	return false
}

//带上下文同步调用，需要排队
func (h *HTaskManager) CallCtx(ctx *TaskCtx, hash uint64, cf interface{}, args ...interface{}) {
	h.incSeed++
	//由上下文信息得出，当前任务来自父任务
	if h.isStuckCall(ctx, hash) {
		h.callInside(ctx, true, hash, cf, args...)
	} else {
		h.hTasks[hash%h.divNum].callCtx(ctx, true, hash, cf, args...)
	}
}

//带上下文同步调用，不需要排队
func (h *HTaskManager) CallCtxR(cf interface{}, args ...interface{}) {
	h.incSeed++
	h.hTasks[h.incSeed%h.divNum].callCtx(nil, false, h.incSeed, cf, args...)
}

//带上下文异步调用，需要排队
func (h *HTaskManager) CallCtxGo(ctx *TaskCtx, hash uint64, cf interface{}, args ...interface{}) {
	h.incSeed++
	h.hTasks[hash%h.divNum].callCtxGo(true, hash, cf, args...)
}

//带上下文异步调用，不需要排队
func (h *HTaskManager) CallCtxGoR(cf interface{}, args ...interface{}) {
	h.incSeed++
	h.hTasks[h.incSeed%h.divNum].callCtxGo(false, h.incSeed, cf, args...)
}

//组调用
func (h *HTaskManager) callGroup(hashKey uint64, cf interface{}, args ...interface{}) chan int {
	h.incSeed++
	return h.hTasks[hashKey%h.divNum].callGroup(true, hashKey, cf, args...)
}

func (h *HTaskManager) callGroupR(cf interface{}, args ...interface{}) chan int {
	h.incSeed++
	return h.hTasks[h.incSeed%h.divNum].callGroup(false, h.incSeed, cf, args...)
}

func (h *HTaskManager) callCtxGroup(ctx *TaskCtx, hashKey uint64, cf interface{}, args ...interface{}) chan int {
	if h.isStuckCall(ctx, hashKey) {
		h.callInside(ctx, true, hashKey, cf, args...)
		return nil
	} else {
		return h.hTasks[hashKey%h.divNum].callCtxGroup(ctx, true, hashKey, cf, args...)
	}
}

func (h *HTaskManager) callCtxGroupR(cf interface{}, args ...interface{}) chan int {
	h.incSeed++
	return h.hTasks[h.incSeed%h.divNum].callGroup(false, h.incSeed, cf, args...)
}

//获取组调用对象
func (h *HTaskManager) FetchTaskGroup() *HTaskGroup {
	var tg HTaskGroup
	tg.h = h
	return &tg
}

//组调用
type groupTaskNode struct {
	hashKey   uint64
	cf        interface{}
	args      []interface{}
	ctx       *TaskCtx
	isCtxCall bool
}

type HTaskGroup struct {
	sync.Mutex
	tasks []*groupTaskNode
	h     *HTaskManager
	wcs   []chan int
}

//调用函数，并保存等待channel
func (hg *HTaskGroup) Call(hash uint64, cf interface{}, args ...interface{}) {
	wc := hg.h.callGroup(hash, cf, args...)
	hg.wcs = append(hg.wcs, wc)
}

//无排队调用
func (hg *HTaskGroup) CallR(cf interface{}, args ...interface{}) {
	wc := hg.h.callGroupR(cf, args...)
	hg.wcs = append(hg.wcs, wc)
}

func (hg *HTaskGroup) CallCtx(ctx *TaskCtx, hash uint64, cf interface{}, args ...interface{}) {
	wc := hg.h.callCtxGroup(ctx, hash, cf, args...)
	hg.Lock()
	if wc != nil {
		hg.wcs = append(hg.wcs, wc)
	}
	hg.Unlock()
}

func (hg *HTaskGroup) CallCtxR(cf interface{}, args ...interface{}) {
	wc := hg.h.callCtxGroupR(cf, args...)
	hg.wcs = append(hg.wcs, wc)
}

//此处执行，并等待结果
func (hg *HTaskGroup) Wait() {
	//等待channel返回
	for _, v := range hg.wcs {
		<-v
	}
}
