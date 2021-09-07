package influxdb

import (
	"errors"
	"strings"
)

func SenmlBasename(name string) (deviceName string, measurement string, thingId string, err error) {
	splitted := strings.Split(name, ":")
	if len(splitted) == 2 {
		deviceName = splitted[0]
		measurement = splitted[1]
	} else if len(splitted) == 3 {
		deviceName = splitted[0]
		measurement = splitted[1]
		thingId = splitted[2]
	} else {
		err = errors.New("malformed message name")
	}
	return
}
