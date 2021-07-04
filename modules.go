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
	DataHandler(msgType int, data interface{})
}

// с помощью интерфейса IServer модуль IModule может:
// - послать данные любому другому модулю
// - перезапустить себя
// - завершить работу всего приложения (в случае критической ошибки)
type IServer interface {
	SendData(name string, msgType int, data interface{}) error
	Restart(module IModule, reason string, timeout time.Duration)
	Terminate(module IModule, reason string, timeout time.Duration)
}

type ModuleCreator func(IServer, string, string) (IModule, error)

type ModuleServer struct {
	mu            sync.Mutex
	modules       map[string]IModule
	moduleCreator ModuleCreator
}

func NewModuleServer(creator ModuleCreator) *ModuleServer {
	return &ModuleServer{
		mu:            sync.Mutex{},
		modules:       make(map[string]IModule),
		moduleCreator: creator,
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

// TODO: добавить параллельный запуск всех модулей
func (ptr *ModuleServer) Start() error {
	ptr.mu.Lock()
	defer ptr.mu.Unlock()

	for id := range ptr.modules {
		if err := ptr.startModule(id); err != nil {
			return err
		}
	}
	return nil
}

// TODO: добавить параллельную остановку всех модулей
func (ptr *ModuleServer) Stop() error {
	ptr.mu.Lock()
	defer ptr.mu.Unlock()

	var errList string
	for id := range ptr.modules {
		if err := ptr.stopModule(id); err != nil {
			errList += "[" + err.Error() + "], "
		}
	}

	if len(errList) != 0 {
		errList = errList[:len(errList)-2]
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

func (ptr *ModuleServer) SendData(id string, msgType int, data interface{}) error {
	module, ok := ptr.modules[id]
	if !ok {
		return errors.New("module " + id + " not found")
	}

	if !module.IsStarted() {
		return errors.New("module " + id + " is not started")
	}

	module.DataHandler(msgType, data)

	return nil
}

func (ptr *ModuleServer) Restart(module IModule, reason string, timeout time.Duration) {
	log.Println("W> module " + string(module.GetID()) + " requested a restart, reason: " + reason)

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

	go func() {
		time.Sleep(timeout)
		if module.IsStarted() {
			TerminateCurrentProcess("timeout " + timeout.String() + " reached while stopping")
		}
	}()
}
