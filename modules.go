package common

import (
	"encoding/json"
	"errors"
	"fmt"
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
}

type ModuleCreator func(ModuleID) (IModule, error)

type ModuleServer struct {
	modules       map[ModuleID]IModule
	moduleCreator ModuleCreator
}

func NewModuleServer(creator ModuleCreator) *ModuleServer {
	return &ModuleServer{
		modules:       make(map[ModuleID]IModule),
		moduleCreator: creator,
	}
}

func (ptr *ModuleServer) LoadConfig(config *ModuleServerConfig) ([]ModuleID, error) {

	mLen := len(config.Modules)
	if mLen == 0 {
		return nil, errors.New("there are no any modules in config")
	}

	modulesID := make([]ModuleID, 0, mLen)

	for _, cfg := range config.Modules {
		if cfg.Disable {
			continue
		}

		newModule, err := ptr.moduleCreator(cfg.ID)
		if err != nil {
			return nil, errors.New("creation module " + string(cfg.ID) + " failed, " + err.Error())
		}

		if err := newModule.LoadConfig(cfg.Params); err != nil {
			return nil, errors.New("loading config for module " + string(cfg.ID) + " failed, " + err.Error())
		}

		ptr.modules[cfg.ID] = newModule

		modulesID = append(modulesID, cfg.ID)
	}

	return modulesID, nil
}

func (ptr *ModuleServer) GetModule(ID ModuleID) IModule {
	return ptr.modules[ID]
}

// TODO: добавить параллельный запуск всех модулей
func (ptr *ModuleServer) Start() error {
	for moduleID := range ptr.modules {
		if err := ptr.startModule(moduleID); err != nil {
			return err
		}
	}
	return nil
}

// TODO: добавить параллельную остановку всех модулей
func (ptr *ModuleServer) Stop() error {
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
