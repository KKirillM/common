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
	Name    string          `json:"name"`
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
	GetName() string
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

type ModuleCreator func(IServer, string) (IModule, error)

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

		newModule, err := ptr.moduleCreator(ptr, cfg.ID)
		if err != nil {
			return nil, errors.New("creation module " + cfg.Name + " failed, " + err.Error())
		}

		ptr.modules[cfg.Name] = newModule

		if err := newModule.LoadConfig(cfg.Params); err != nil {
			return nil, errors.New("loading config for module " + cfg.Name + " failed, " + err.Error())
		}

		modulesList = append(modulesList, cfg.ID)
	}

	return modulesList, nil
}

// TODO: добавить параллельный запуск всех модулей
func (ptr *ModuleServer) Start() error {
	ptr.mu.Lock()
	defer ptr.mu.Unlock()

	for moduleID := range ptr.modules {
		if err := ptr.startModule(moduleID); err != nil {
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
	for moduleID := range ptr.modules {
		if err := ptr.stopModule(moduleID); err != nil {
			errList += "[" + err.Error() + "], "
		}
	}

	if len(errList) != 0 {
		errList = errList[:len(errList)-2]
		return errors.New(errList)
	}

	return nil
}

func (ptr *ModuleServer) startModule(name string) error {
	module, ok := ptr.modules[name]
	if !ok {
		return errors.New("module " + name + " not found")
	}

	if module.IsStarted() {
		return errors.New("module " + name + " already started")
	}

	return module.Start()
}

func (ptr *ModuleServer) stopModule(name string) error {
	module, ok := ptr.modules[name]
	if !ok {
		return errors.New("module " + name + " not found")
	}

	if !module.IsStarted() {
		return errors.New("module " + name + " already stopped")
	}

	return module.Stop()
}

func (ptr *ModuleServer) SendData(name string, msgType int, data interface{}) error {
	module, ok := ptr.modules[name]
	if !ok {
		return errors.New("module " + name + " not found")
	}

	if !module.IsStarted() {
		return errors.New("module " + name + " is not started")
	}

	module.DataHandler(msgType, data)

	return nil
}

func (ptr *ModuleServer) Restart(module IModule, reason string, timeout time.Duration) {
	log.Println("W> module " + string(module.GetName()) + " requested a restart, reason: " + reason)

	if err := module.Stop(); err != nil {
		TerminateCurrentProcess("module '" + module.GetName() + "' stop failed: " + err.Error())
		return
	}

	if err := module.Start(); err != nil {
		TerminateCurrentProcess("module '" + module.GetName() + "' start failed: " + err.Error())
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
	log.Println("E> module " + string(module.GetName()) + " requested a stop, reason: " + reason)
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
