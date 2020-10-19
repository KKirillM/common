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
	IsStart() bool
}

type ModuleCreator func() IModule

type ModuleServer struct {
	modules        map[ModuleID]IModule
	moduleCreators map[ModuleID]ModuleCreator
}

func NewModuleServer() *ModuleServer {
	return &ModuleServer{
		modules:        make(map[ModuleID]IModule),
		moduleCreators: make(map[ModuleID]ModuleCreator),
	}
}

func (ptr *ModuleServer) AddModuleCreator(ID ModuleID, creator ModuleCreator) {
	ptr.moduleCreators[ID] = creator
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

		creator, ok := ptr.moduleCreators[cfg.ID]
		if !ok {
			return nil, errors.New("creation function for module " + string(cfg.ID) + " not found")
		}

		newModule := creator()
		if newModule == nil {
			return nil, errors.New("creation module " + string(cfg.ID) + " failed")
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

func (ptr *ModuleServer) StartModule(ID ModuleID) error {
	module, ok := ptr.modules[ID]
	if !ok {
		return errors.New("module " + string(ID) + " not found")
	}

	if module.IsStart() {
		return errors.New("module " + string(ID) + " already started")
	}

	return module.Start()
}

func (ptr *ModuleServer) StopModule(ID ModuleID) error {
	module, ok := ptr.modules[ID]
	if !ok {
		return errors.New("module " + string(ID) + " not found")
	}

	if !module.IsStart() {
		return errors.New("module " + string(ID) + " already stopped")
	}

	return module.Stop()
}
