package influxdb

import (
	"errors"
	"strings"
)

func SenmlBasename(name string) (deviceName string, measurement string, err error) {
	splitted := strings.Split(name, ":")
	if len(splitted) == 2 {
		deviceName = splitted[0]
		measurement = splitted[1]
	} else {
		err = errors.New("malformed message name")
	}
	return
}
