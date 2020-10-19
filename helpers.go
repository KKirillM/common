package common

import (
	"fmt"
	"os"
	"strings"
)

func LoadConfigFile(path string) (string, error) {

	if len(path) == 0 {
		applicaitonName, err := os.Executable()
		if err != nil {
			return "", fmt.Errorf("can't get config file name, %v", err)
		}
		s := strings.Split(applicaitonName, "\\")
		s = strings.Split(s[len(s)-1], ".")
		applicaitonName = s[0]
		path = applicaitonName + ".config.json"
	}

	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("can't open config file: %s, %v", path, err)
	}
	defer file.Close()

	var bytes []byte
	if _, err = file.Read(bytes); err != nil {
		return "", fmt.Errorf("can't read config file: %s, %v", path, err)
	}

	return string(bytes), nil
}
