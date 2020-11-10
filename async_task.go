package common

import (
	"context"
	"errors"
	"log"
	"time"
)

type IAsyncTask interface {
	Execute()
	Break()
	BreakAndWait()
}

type managedObject struct {
	breakChan  chan struct{}
	finishChan chan struct{}
}

func newManagedObject() managedObject {
	return managedObject{
		breakChan:  make(chan struct{}),
		finishChan: make(chan struct{}),
	}
}

func (ptr *managedObject) Break() {
	if !ptr.IsStoped() {
		close(ptr.breakChan)
	}
}

func (ptr *managedObject) BreakAndWait() {
	if !ptr.IsStoped() {
		close(ptr.breakChan)
		<-ptr.finishChan
	}
}

func (ptr *managedObject) IsStoped() bool {
	select {
	case <-ptr.breakChan:
		return true
	default:
		return false
	}
}

/*
AsyncTask
*/

type AsyncTaskFunc = func(breakChan <-chan struct{})

type asyncTask struct {
	managedObject
	task AsyncTaskFunc
}

func NewAsyncTask(task AsyncTaskFunc) IAsyncTask {
	return &asyncTask{
		managedObject: newManagedObject(),
		task:          task,
	}
}

func (ptr *asyncTask) Execute() {
	go func() {
		ptr.task(ptr.breakChan)
		close(ptr.finishChan)
	}()
}

/*
RepeatableTask
*/

type RepeatableTaskFunc = func()

type repeatableTask struct {
	managedObject
	task    RepeatableTaskFunc
	timeout time.Duration
}

func NewRepeatableTask(task RepeatableTaskFunc, timeout time.Duration) IAsyncTask {
	return &repeatableTask{
		managedObject: newManagedObject(),
		task:          task,
		timeout:       timeout,
	}
}

func (ptr *repeatableTask) Execute() {
	go func() {
		defer close(ptr.finishChan)
		timer := time.NewTimer(ptr.timeout)
		for {
			ptr.task()

			select {
			case <-timer.C:
				return
			case <-ptr.breakChan:
				return
			default:
				timer.Reset(ptr.timeout)
			}
		}
	}()
}

/*
TasksExecutor
*/

type TasksExecutor struct {
	managedObject
	tasks     chan func()
	terminate bool
}

func NewTasksExecutor(queueSize int) *TasksExecutor {
	return &TasksExecutor{
		managedObject: newManagedObject(),
		tasks:         make(chan func(), queueSize),
	}
}

func (ptr *TasksExecutor) Run() {
	go ptr.executionCycle()
}

func (ptr *TasksExecutor) Terminate() {
	if !ptr.IsStoped() {
		ptr.terminate = true
		ptr.Break()
	}
}

func (ptr *TasksExecutor) Execute(taskName string, task func()) error {
	if ptr.IsStoped() {
		return errors.New("tasks executor stopped")
	}

	select {
	case ptr.tasks <- task:
	default:
		return errors.New("execute " + taskName + " task failed, tasks queue is full")
	}

	return nil
}

func (ptr *TasksExecutor) ExecuteAnyway(ctx context.Context, taskName string, task func()) error {
	if ptr.IsStoped() {
		return errors.New("tasks executor stopped")
	}

	if len(ptr.tasks) == cap(ptr.tasks) {
		log.Println("W> tasks channel is full, task '" + taskName + "' execution may be delayed")
	}

	select {
	case ptr.tasks <- task:
	case <-ctx.Done():
	}

	return nil
}

func (ptr *TasksExecutor) ExecuteAndWait(taskName string, task func()) error {
	done := make(chan struct{}, 1)

	err := ptr.Execute(taskName, func() {
		task()
		done <- struct{}{}
	})

	if err != nil {
		return err
	}

	<-done
	return nil
}

func (ptr *TasksExecutor) ExecuteAndWaitError(taskName string, task func() error) error {
	result := make(chan error, 1)

	err := ptr.Execute(taskName, func() {
		result <- task()
	})

	if err != nil {
		return err
	}

	return <-result
}

func (ptr *TasksExecutor) executionCycle() {

	// очерёдность исполнения defer функций происходит в порядке обратном объявлению, т.е. эта функция исполнится последней
	defer close(ptr.finishChan)

	// завершение обработки всех задач находящихся в очереди на момент остановки
	defer func() {
		if ptr.terminate {
			return
		}
		for {
			select {
			case task := <-ptr.tasks:
				{
					task()
				}
			default:
				return
			}
		}
	}()

	for {
		select {
		case task := <-ptr.tasks:
			{
				task()
			}
		case <-ptr.breakChan:
			return
		}
	}
}

func ExecuteWithTimeout(timeout time.Duration, task, onTimeout func()) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	executed := make(chan struct{}, 1)

	go func() {
		task()
		executed <- struct{}{}
	}()

	select {
	case <-executed:
	case <-timer.C:
		onTimeout()
	}
}
