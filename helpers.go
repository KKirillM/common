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

func BaseCurrency(symbol string) string {
	symbols := strings.Split(symbol, "/")
	if len(symbols) != 2 {
		return ""
	}
	return symbols[0]
}

func QuoteCurrency(symbol string) string {
	symbols := strings.Split(symbol, "/")
	if len(symbols) != 2 {
		return ""
	}
	return symbols[1]
}

func SliceUnion(a, b []string) (c []string) {

	m := make(map[string]bool)

	for _, item := range a {
		m[item] = true
		c = append(c, item)
	}

	for _, item := range b {
		if _, ok := m[item]; !ok {
			c = append(c, item)
		}
	}

	return
}

func SliceIntersection(a, b []string) (c []string) {

	m := make(map[string]struct{})

	for _, item := range a {
		m[item] = struct{}{}
	}

	for _, item := range b {
		if _, ok := m[item]; ok {
			c = append(c, item)
		}
	}

	return
}

func SliceDifference(a, b []string) (c []string) {

	m := make(map[string]struct{})

	for _, item := range a {
		m[item] = struct{}{}
	}

	for _, item := range b {
		if _, ok := m[item]; !ok {
			c = append(c, item)
		}
	}

	return
}
