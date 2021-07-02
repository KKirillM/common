package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

type ModuleID string

type ModuleConfig struct {
	ID      ModuleID        `json:"id"`
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
	GetID() ModuleID
	IsStarted() bool
	DataHandler(msgType int, data interface{})
}

// с помощью интерфейса IServer модуль IModule может:
// - послать данные любому другому модулю
// - перезапустить себя
// - завершить работу всего приложения (в случае критической ошибки)
type IServer interface {
	SendData(ID ModuleID, msgType int, data interface{}) error
	Restart(module IModule, reason string, timeout time.Duration)
	Terminate(module IModule, reason string, timeout time.Duration)
}

type ModuleCreator func(IServer, ModuleID) (IModule, error)

type ModuleServer struct {
	mu            sync.Mutex
	modules       map[ModuleID]IModule
	moduleCreator ModuleCreator
}

func NewModuleServer(creator ModuleCreator) *ModuleServer {
	return &ModuleServer{
		mu:            sync.Mutex{},
		modules:       make(map[ModuleID]IModule),
		moduleCreator: creator,
	}
}

func (ptr *ModuleServer) LoadConfig(config *ModuleServerConfig) ([]ModuleID, error) {

	mLen := len(config.Modules)
	if mLen == 0 {
		return nil, errors.New("there are no any modules in config")
	}

	modulesIDList := make([]ModuleID, 0, mLen)

	for _, cfg := range config.Modules {
		if cfg.Disable {
			continue
		}

		newModule, err := ptr.moduleCreator(ptr, cfg.ID)
		if err != nil {
			return nil, errors.New("creation module " + string(cfg.ID) + " failed, " + err.Error())
		}

		ptr.modules[cfg.ID] = newModule

		if err := newModule.LoadConfig(cfg.Params); err != nil {
			return nil, errors.New("loading config for module " + string(cfg.ID) + " failed, " + err.Error())
		}

		modulesIDList = append(modulesIDList, cfg.ID)
	}

	return modulesIDList, nil
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

func (ptr *ModuleServer) startModule(ID ModuleID) error {
	module, ok := ptr.modules[ID]
	if !ok {
		return errors.New("module " + string(ID) + " not found")
	}

	if module.IsStarted() {
		return errors.New("module " + string(ID) + " already started")
	}

	return module.Start()
}

func (ptr *ModuleServer) stopModule(ID ModuleID) error {
	module, ok := ptr.modules[ID]
	if !ok {
		return errors.New("module " + string(ID) + " not found")
	}

	if !module.IsStarted() {
		return errors.New("module " + string(ID) + " already stopped")
	}

	return module.Stop()
}

func (ptr *ModuleServer) SendData(ID ModuleID, msgType int, data interface{}) error {
	module, ok := ptr.modules[ID]
	if !ok {
		return errors.New("module " + string(ID) + " not found")
	}

	if !module.IsStarted() {
		return errors.New("module " + string(ID) + " is not started")
	}

	module.DataHandler(msgType, data)

	return nil
}

func (ptr *ModuleServer) Restart(module IModule, reason string, timeout time.Duration) {
	log.Println("W> module " + string(module.GetID()) + " requested a restart, reason: " + reason)

	if err := module.Stop(); err != nil {
		TerminateCurrentProcess("module '" + string(module.GetID()) + "' stop failed: " + err.Error())
		return
	}

	if err := module.Start(); err != nil {
		TerminateCurrentProcess("module '" + string(module.GetID()) + "' start failed: " + err.Error())
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
