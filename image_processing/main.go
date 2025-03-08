package main

import "fmt"

type Job struct {
	inPath string
	outPath string
}

func loadImg(paths []string) <-chan Job {
	out := make(chan Job)
	go func ()  {
		for _,p := range paths {
			job := Job{
				inPath: p,
				outPath: "/abc",
			}
			out <- job
		}
		close(out)
	}()
	return out
}

func resize(input <-chan Job) <-chan Job {
	out := make(chan Job)
	go func ()  {
		for i := range input {
			out <- i
		}
		close(out)	
	}()
	return out
}

func convert(input <-chan Job) <-chan Job {
	out := make(chan Job)
	go func ()  {
		for i := range input {
			out <- i
		}
		close(out)	
	}()
	return out
}

func save(input <-chan Job) <-chan bool {
	out := make(chan bool)
	go func ()  {
		for _ = range input {
			out <- true
		}
		close(out)	
	}()
	return out
}

func main() {
	paths := []string {
		"abc","abc",
	}

	chan1 := loadImg(paths)
	chan2 := resize(chan1)
	chan3 := convert(chan2)
	res := save(chan3)

	for range res {
		fmt.Println("done")
	}
}