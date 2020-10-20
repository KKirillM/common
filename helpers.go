package common

import (
	"fmt"
	"io/ioutil"
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
		cwd, _ := os.Getwd()
		path = cwd + "\\" + path
	}

	content, err := ioutil.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("can't open config file: %s, %v", path, err)
	}

	return string(content), nil
}
