package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

type ModuleConfig struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Disable bool            `json:"disable"`
	Params  json.RawMessage `json:"params"`
}

type ModuleServerConfig struct {
	Modules []ModuleConfig `json:"modules"`
}

func ParseModuleServerConfig(config string) (*ModuleServerConfig, error) {
	cfg := ModuleServerConfig{}

	if err := json.Unmarshal([]byte(config), &cfg); err != nil {
		return nil, fmt.Errorf("can't decode config JSON, %v", err)
	}

	return &cfg, nil
}

type IModule interface {
	LoadConfig(config json.RawMessage) error
	Start() error
	Stop() error
	GetID() string
	GetType() string
	IsStarted() bool
	DataHandler(msgType int, data interface{}) error
}

// с помощью интерфейса IServer модуль IModule может:
// - послать данные любому другому модулю
// - перезапустить себя
// - завершить работу всего приложения (в случае критической ошибки)
type IServer interface {
	CallModule(moduleID string, msgType int, data interface{}) error
	RestartModule(moduleID string, reason string, timeout time.Duration)
	Terminate(module IModule, reason string, timeout time.Duration)
}

type ModuleCreator func(IServer, string, string) (IModule, error)

type ModuleServer struct {
	mu            sync.Mutex
	modules       map[string]IModule
	moduleCreator ModuleCreator
	//interruptChan chan os.Signal
}

func NewModuleServer(creator ModuleCreator) *ModuleServer {
	return &ModuleServer{
		mu:            sync.Mutex{},
		modules:       make(map[string]IModule),
		moduleCreator: creator,
		//interruptChan: make(chan os.Signal, 1),
	}
}

func (ptr *ModuleServer) LoadConfig(config *ModuleServerConfig) ([]string, error) {

	mLen := len(config.Modules)
	if mLen == 0 {
		return nil, errors.New("there are no any modules in config")
	}

	modulesList := make([]string, 0, mLen)

	for _, cfg := range config.Modules {
		if cfg.Disable {
			continue
		}

		newModule, err := ptr.moduleCreator(ptr, cfg.Type, cfg.ID)
		if err != nil {
			return nil, errors.New("creation module " + cfg.ID + " failed, " + err.Error())
		}

		ptr.modules[cfg.ID] = newModule

		if err := newModule.LoadConfig(cfg.Params); err != nil {
			return nil, errors.New("loading config for module " + cfg.ID + " failed, " + err.Error())
		}

		modulesList = append(modulesList, cfg.ID)
	}

	return modulesList, nil
}

func (ptr *ModuleServer) Start() error {
	pool := NewJobPool(len(ptr.modules))
	errorsQueue := make(chan error, len(ptr.modules))

	ptr.mu.Lock()

	for id := range ptr.modules {
		func(moduleID string) {
			pool.AddJob(func() {
				errorsQueue <- ptr.startModule(moduleID)
			})
		}(id)
	}
	pool.WaitAll()
	pool.Release()

	ptr.mu.Unlock()

	close(errorsQueue)

	var errList string
	for err := range errorsQueue {
		if err != nil {
			if len(errList) > 0 {
				errList += ", "
			}
			errList += "[" + err.Error() + "]"
		}
	}

	if len(errList) > 0 {
		return errors.New(errList)
	}

	return nil
}

func (ptr *ModuleServer) Stop() error {
	pool := NewJobPool(len(ptr.modules))
	errorsQueue := make(chan error, len(ptr.modules))

	ptr.mu.Lock()

	for id := range ptr.modules {
		func(moduleID string) {
			pool.AddJob(func() {
				errorsQueue <- ptr.stopModule(moduleID)
			})
		}(id)
	}
	pool.WaitAll()
	pool.Release()

	ptr.mu.Unlock()

	close(errorsQueue)

	var errList string
	for err := range errorsQueue {
		if err != nil {
			if len(errList) > 0 {
				errList += ", "
			}
			errList += "[" + err.Error() + "]"
		}
	}

	if len(errList) > 0 {
		return errors.New(errList)
	}

	return nil
}

func (ptr *ModuleServer) startModule(id string) error {
	module, ok := ptr.modules[id]
	if !ok {
		return errors.New("module " + id + " not found")
	}

	if module.IsStarted() {
		return errors.New("module " + id + " already started")
	}

	return module.Start()
}

func (ptr *ModuleServer) stopModule(id string) error {
	module, ok := ptr.modules[id]
	if !ok {
		return errors.New("module " + id + " not found")
	}

	if !module.IsStarted() {
		return errors.New("module " + id + " already stopped")
	}

	return module.Stop()
}

func (ptr *ModuleServer) CallModule(id string, msgType int, data interface{}) error {
	module, ok := ptr.modules[id]
	if !ok {
		return errors.New("module " + id + " not found")
	}

	if !module.IsStarted() {
		return errors.New("module " + id + " is not started")
	}

	return module.DataHandler(msgType, data)
}

func (ptr *ModuleServer) RestartModule(id string, reason string, timeout time.Duration) {
	log.Println("W> module " + id + " requested a restart, reason: " + reason)

	module, ok := ptr.modules[id]
	if !ok {
		log.Println("E> module " + id + " not found")
	}

	if err := module.Stop(); err != nil {
		TerminateCurrentProcess("module '" + module.GetID() + "' stop failed: " + err.Error())
		return
	}

	if err := module.Start(); err != nil {
		TerminateCurrentProcess("module '" + module.GetID() + "' start failed: " + err.Error())
		return
	}

	go func() {
		time.Sleep(timeout)
		if !module.IsStarted() {
			TerminateCurrentProcess("timeout " + timeout.String() + " reached while restarting")
		}
	}()
}

func (ptr *ModuleServer) Terminate(module IModule, reason string, timeout time.Duration) {
	log.Println("E> module " + string(module.GetID()) + " requested a stop, reason: " + reason)

	if err := ptr.Stop(); err != nil {
		TerminateCurrentProcess("some modules stop failed: " + err.Error())
		return
	}

	TerminateCurrentProcess("all modules stopped correctly")

	go func() {
		time.Sleep(timeout)
		if !module.IsStarted() {
			TerminateCurrentProcess("timeout " + timeout.String() + " reached while stopping")
		}
	}()
}
