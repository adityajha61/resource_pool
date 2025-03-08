package main

import (
	"fmt"
	"sync"
	"time"
)


type Task interface {
	Process()
}

type EmailTask struct {
	Email string
	Body string
}

type ImageTask struct {
	ImageUrl string
}

func (t *EmailTask) Process() {
	//processing email
	time.Sleep(2*time.Second)
	fmt.Println("done processing email")
}

func (t *ImageTask) Process() {
	//processingImage
	time.Sleep(2*time.Second)
	fmt.Println("processing image done")
}

type WorkerPool struct {
	Tasks []Task
	concurrency int
	tasksChan chan Task
	wg sync.WaitGroup
}

// func(t *Task) Process() {
// 	fmt.Print("processing")
// 	time.Sleep(2*time.Second)
// }

func (wp *WorkerPool) worker() {
	for task := range wp.tasksChan {
		task.Process()
		wp.wg.Done()
	}
}

func (wp *WorkerPool) Run() {
	wp.tasksChan = make(chan Task, len(wp.Tasks))

	for i:=0; i< wp.concurrency; i++ {
		go wp.worker()
	}

	wp.wg.Add(len(wp.Tasks))
	fmt.Print("start at ",time.Now())
	start := time.Now().Second()
	for _, task := range wp.Tasks {
		wp.tasksChan <- task
	}
	close(wp.tasksChan)
	wp.wg.Wait()
	end:= time.Now().Second()
	fmt.Print("finished at ", time.Now())
	fmt.Print("time taken", end-start)
}

func main() {
	tasks := make([]Task , 20)
	
	for i:=0 ;i<10;i++{
		tasks[i] = &EmailTask{
			Email: "abc",
			Body: "abc",
		}
	}

	for i:=10 ;i<20;i++{
		tasks[i] = &ImageTask{
			ImageUrl: "abc",
		}
	}

	wp := &WorkerPool{
		Tasks: tasks,
		concurrency: 5,
	}
	
	wp.Run()


}