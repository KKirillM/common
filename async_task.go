package common

type IAsyncTask interface {
	Execute()
	Break()
}

type TaskFunc = func(breakChan <-chan struct{})

type asyncTask struct {
	task       TaskFunc
	breakChan  chan struct{}
	finishChan chan struct{}
}

func NewAsyncTask(task TaskFunc) IAsyncTask {
	return &asyncTask{
		task:       task,
		breakChan:  make(chan struct{}),
		finishChan: make(chan struct{}),
	}
}

func (ptr *asyncTask) Execute() {
	go func() {
		ptr.task(ptr.breakChan)
		close(ptr.finishChan)
	}()
}

func (ptr *asyncTask) Break() {
	close(ptr.breakChan)
	<-ptr.finishChan
}
