package main

import (
	"fmt"
	"reflect"
	"sync"
	"time"
)

const (
	SystemTotalMem = 100
	TotalCPU = 100
)

type Query struct {
	ID string
	CPUReq int
	MemReq int
	ExecTime time.Duration
	Timeout time.Duration
}

type ResourcePool struct {
	Name string
	MaxCPU int
	MaxMemoryPer int
	UsedCPU int
	UsedMemory int
	MaxConcurrency int
	ActiveQueries chan struct{}
	mu sync.Mutex
}

type PoolManager struct {
	pools map[string]*ResourcePool
	mu sync.Mutex
}

func NewPoolManager() *PoolManager {
	return &PoolManager{
		pools: make(map[string]*ResourcePool),
	}
}

func (pm *PoolManager) AddPool(pool *ResourcePool){
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.pools[pool.Name] = pool
}

func (pm *PoolManager) GetPool(name string) (*ResourcePool,bool){
	pm.mu.Lock()
	defer pm.mu.Unlock()
	p,ok := pm.pools[name]
	return p,ok
}

func (pm *PoolManager) AlterPool(newPool *ResourcePool) bool {
	oldPool,ok := pm.GetPool(newPool.Name)
	if !ok {
		return false
	}
	oldPool.mu.Lock()
	defer oldPool.mu.Unlock()
	if newPool.MaxCPU > 0 {
		oldPool.MaxCPU = newPool.MaxCPU
	}
	if newPool.MaxMemoryPer > 0 {
		oldPool.MaxMemoryPer = newPool.MaxMemoryPer
	}
	if newPool.MaxConcurrency > 0 && newPool.MaxConcurrency != oldPool.MaxConcurrency {
		close(oldPool.ActiveQueries)
		tempChan := make(chan struct{},oldPool.MaxConcurrency)
		// newActive := make(chan struct{}, newPool.MaxConcurrency)
		for range oldPool.ActiveQueries {
			tempChan <- <-oldPool.ActiveQueries
		}
		oldPool.ActiveQueries = make(chan struct{}, newPool.MaxConcurrency)
		// n:= len(tempChan)
		for i:=0;i<len(tempChan);i++ {
			oldPool.ActiveQueries <- <-tempChan
		}
		oldPool.MaxConcurrency = newPool.MaxConcurrency
	}
	return true
}

func NewResourcePool(name string, maxCPU int, maxMemory int, maxQueries int) *ResourcePool {
	resourcePool := &ResourcePool{
		Name: name,
		MaxCPU: maxCPU,
		MaxMemoryPer: maxMemory,
		MaxConcurrency: maxQueries,
		ActiveQueries: make(chan struct{},maxQueries),
	}
	return resourcePool
}

func (p *ResourcePool) SubmitQuery(query Query, wg *sync.WaitGroup) {
	defer wg.Done()
	p.ActiveQueries <- struct{}{}
	defer func() { <- p.ActiveQueries } ()
	p.mu.Lock()

	poolMemLimit := (p.MaxMemoryPer * SystemTotalMem) / 100;
	poolCpuLimit := (p.MaxCPU * SystemTotalMem) / 100;

	if (p.UsedCPU + query.CPUReq > poolCpuLimit || p.UsedMemory + query.MemReq > poolMemLimit){
		p.mu.Unlock()
		// fmt.Printf("not enough cpu/mem")
		return
	}

	p.UsedCPU += query.CPUReq
	p.UsedMemory += query.MemReq

	// p.mu.Unlock()

	//doing query exec
	// time.Sleep(1*time.Second)
	if(query.ExecTime > query.Timeout){
		fmt.Printf("timedout %s", query.ID)
	}

	// p.mu.Lock()
	p.UsedCPU -= query.CPUReq
	p.UsedMemory -= query.MemReq
	p.mu.Unlock()

}

func (pm *PoolManager) SubmitQueryToPool(name string, query Query, wg *sync.WaitGroup) {
	pool,exists := pm.GetPool(name)
	if !exists {
		wg.Done()
		return
	}
	go pool.SubmitQuery(query,wg)
}

func (pm *PoolManager) ShowPools() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for name,pool := range pm.pools {
		pool.mu.Lock()
		// copy := ResourcePool{
		// 	Name: pool.Name,
		// 	MaxCPU: pool.MaxCPU,
		// 	MaxMemoryPer: pool.MaxMemoryPer,
		// 	MaxConcurrency: pool.MaxConcurrency,
		// 	UsedCPU: pool.UsedCPU,
		// 	UsedMemory: pool.UsedMemory,
		// 	ActiveQueries: pool.ActiveQueries,
		// }
		types := reflect.TypeOf(pool).Elem()
		copy := reflect.New(types).Elem()
		org := reflect.ValueOf(pool)
		for i:=0;i< copy.NumField();i++ {
			field := copy.Type().Field(i)
			if field.Type != reflect.TypeOf(sync.Mutex{}) {
				copy.Field(i).Set(org.Elem().Field(i))
			}
		}
		// copy.mu = sync.Mutex{}
		pool.mu.Unlock()
		fmt.Print("name = ",name)
		fmt.Println("pool = " ,copy.Interface())
	}
}

func (pm *PoolManager) DropPool(name string) bool {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pool,ok := pm.GetPool(name)
	if !ok {
		return false
	}
	
	select {
	case <- pool.ActiveQueries :
			fmt.Println("waiting for queries to execute")
	default:
		close(pool.ActiveQueries)
	}

	delete(pm.pools,name)
	return true
}

func main() {
	pm := NewPoolManager()

	rpool := NewResourcePool("test",60,65,40)
	rpool2 := NewResourcePool("test2",60,75,35)
	
	pm.AddPool(rpool)
	pm.AddPool(rpool2)

	queries := []struct {
		name string
		query Query
	}{
		{"test", Query{"q1",10,20,1*time.Second,2*time.Second}},
		{"test2", Query{"q2",12,25,1*time.Second,2*time.Second},},
	}

	var wg sync.WaitGroup
	for _,q := range queries {
		for i:=0;i<=1000;i++ {
			wg.Add(4)
			go pm.SubmitQueryToPool(q.name, q.query,&wg)
			go pm.SubmitQueryToPool(q.name, q.query,&wg)
			go pm.SubmitQueryToPool(q.name, q.query,&wg)
			go pm.SubmitQueryToPool(q.name, q.query,&wg)
		}
	}
	newConfig := &ResourcePool{
		Name:          "test", // Existing pool name
		MaxCPU:        80,     // Update CPU limit
		MaxMemoryPer:  70,     // Update Memory limit
		MaxConcurrency: 50,    // Update max concurrent queries
	}
	// old,_ := pm.GetPool(newConfig.Name)
	fmt.Println( " show pools ")
	pm.ShowPools()
	pm.AlterPool(newConfig)
	fmt.Println( " show pools ")
	pm.ShowPools()
	// new,_ := pm.GetPool(newConfig.Name)
	// fmt.Println( " new pool = ", new)
	wg.Wait()
	// time.Sleep(4*time.Second)
	fmt.Print(SystemTotalMem,newConfig)
}