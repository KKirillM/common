package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type UniAddresses struct {
	Factory string `json:"factory"`
	Router  string `json:"router"`
}

type Configuration struct {
	Name             string       `json:"name"`
	NodeAddress      string       `json:"node_address"`
	UniswapAddresses UniAddresses `json:"uniswap_addresses"`
}

func NewConfiguration() (*Configuration, error) {

	var path, applicaitonName string

	if len(os.Args) > 1 {
		path = os.Args[1]
	}

	var err error
	if applicaitonName, err = os.Executable(); err != nil {
		return nil, fmt.Errorf("can't get config file name, %v", err)
	}
	s := strings.Split(applicaitonName, "\\")
	s = strings.Split(s[len(s)-1], ".")
	applicaitonName = s[0]

	if len(path) == 0 {
		path = applicaitonName + ".cfg.json"
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("can't open config file: %s, %v", path, err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	cfg := Configuration{}
	err = decoder.Decode(&cfg)
	if err != nil {
		return nil, fmt.Errorf("can't decode config JSON, %v", err)
	}

	if cfg.Name != applicaitonName {
		return nil, fmt.Errorf("wrong config file, application_name=%s but config_name=%s", applicaitonName, cfg.Name)
	}

	return &cfg, nil
}
