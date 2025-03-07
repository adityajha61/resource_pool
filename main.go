package main

import (
	"fmt"
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

func NewResourcePool(name string, maxCPU int, maxMemory int, maxQueries int) *ResourcePool {
	resourcePool := &ResourcePool{
		Name: name,
		MaxCPU: maxCPU,
		MaxMemoryPer: maxMemory,
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

	p.mu.Unlock()

	//doing query exec
	if(query.ExecTime > query.Timeout){
		fmt.Printf("timedout %d", query.ID)
	}

	p.mu.Lock()
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
		{"rpool", Query{"q1",10,20,1*time.Second,2*time.Second}},
		{"rpool2", Query{"q2",12,25,1*time.Second,2*time.Second},},
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
	wg.Wait()
	// time.Sleep(4*time.Second)
	fmt.Print(SystemTotalMem)
}