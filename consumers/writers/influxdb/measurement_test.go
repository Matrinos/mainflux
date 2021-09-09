package influxdb_test

import (
	"fmt"
	"testing"

	influxdb "github.com/mainflux/mainflux/consumers/writers/influxdb"
	"github.com/stretchr/testify/assert"
)

func TestSenmlBasename(t *testing.T) {

	_, _, err := influxdb.SenmlBasename("test")

	assert.Error(t, err, fmt.Sprintf("Parse name expected to fail: %s.\n", err))

	baseName1, measurement1, err1 := influxdb.SenmlBasename("base-name:measurement")
	assert.Nil(t, err1, fmt.Sprintf("Parse name expected to succeed: %s.\n", err1))
	assert.Equal(t, "base-name", baseName1, fmt.Sprintf("Parse name expected get base name successfully: %s.\n", baseName1))
	assert.Equal(t, "measurement", measurement1, fmt.Sprintf("Parse name expected get measurement successfully: %s.\n", measurement1))

	_, _, err3 := influxdb.SenmlBasename("base-name:measurement:thing-id:some-other-value")
	assert.Error(t, err3, fmt.Sprintf("Parse name expected to fail: %s.\n", err3))
}
