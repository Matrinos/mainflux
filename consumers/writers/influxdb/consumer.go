// Copyright (c) Mainflux
// SPDX-License-Identifier: Apache-2.0

package influxdb

import (
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	influxdata "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/mainflux/mainflux/consumers"
	log "github.com/mainflux/mainflux/logger"
	"github.com/mainflux/mainflux/pkg/errors"
	"github.com/mainflux/mainflux/pkg/transformers/json"
	"github.com/mainflux/mainflux/pkg/transformers/senml"
	"github.com/mainflux/mainflux/things"
)

var logger, _ = log.New(os.Stdout, log.Info.String())

var errSaveMessage = errors.New("failed to save message to influxdb database")

var _ consumers.Consumer = (*influxRepo)(nil)

type influxRepo struct {
	client           influxdata.Client
	bucket           string
	org              string
	writeAPI         api.WriteAPI
	mainfluxApiToken string
	thingsService    *ThingsService
	logger           log.Logger
}

// New returns new InfluxDB writer.
func New(client influxdata.Client, org, bucket, mainfluxApiToken, mainfluxUrl string, logger log.Logger) consumers.Consumer {
	thingsServiceConfig := &ThingsServiceConfig{
		Token: mainfluxApiToken,
		Url:   mainfluxUrl,
	}

	writeAPI := client.WriteAPI(org, bucket)
	errorsCh := writeAPI.Errors()

	go func() {
		for err := range errorsCh {
			logger.Error(fmt.Sprintf("Failed to write the data to influxdb :: %s", err))
		}
	}()

	return &influxRepo{
		client:           client,
		bucket:           bucket,
		org:              org,
		writeAPI:         writeAPI,
		mainfluxApiToken: mainfluxApiToken,
		thingsService:    NewThingsService(thingsServiceConfig),
		logger:           logger,
	}
}

func (repo *influxRepo) Consume(message interface{}) error {

	switch m := message.(type) {
	// TODO: Bring back json support
	case json.Messages:
		if strings.ToUpper(m.Format) == "JSON" {
			return repo.jsonPoints(m)
		} else {
			// TODO: shall we actually return an error here.
			// maybe this kinds payload is for other consumers?
			repo.logger.Info(fmt.Sprintf("Ignore the message with format %s", m.Format))
		}
	default:
		return repo.senmlPoints(m)
	}

	return nil
}

func (repo *influxRepo) senmlPoints(messages interface{}) error {
	msgs, ok := messages.([]senml.Message)
	if !ok {
		return errSaveMessage
	}

	for _, msg := range msgs {
		deviceName, measurement, err := SenmlBasename(msg.Name)

		if err != nil {
			deviceName = msg.Name
			measurement = msg.Name
		}

		// if there is err of getting meta, ignore it
		// still save it to influx db
		thingID := msg.Link

		if thingID == "" {
			thingID = msg.Publisher
		}
		meta, _ := repo.thingsService.GetThingMetaById(thingID)

		tgs := senmlTags(msg, deviceName, meta)

		flds := senmlFields(msg)

		sec, dec := math.Modf(msg.Time)
		t := time.Unix(int64(sec), int64(dec*(1e9)))
		pt := influxdata.NewPoint(measurement, tgs, flds, t)
		repo.writeAPI.WritePoint(pt)
	}
	repo.writeAPI.Flush()
	return nil
}

// TODO: Bring back json support
func (repo *influxRepo) jsonPoints(msgs json.Messages) error {
	for _, m := range msgs.Data {
		flat, err := json.Flatten(m.Payload)
		if err != nil {
			return errors.Wrap(json.ErrTransform, err)
		}

		measurement := ""
		deviceName := ""
		// Copy first-level fields so that the original Payload is unchanged.
		fields := make(map[string]interface{})
		for k, v := range flat {
			if k == "deviceName" {
				deviceName = v.(string)
			} else if k == "measurement" {
				measurement = v.(string)
			} else {
				fields[k] = v
			}

		}

		if measurement == "" {
			// if no measurement, ignore the message.
			// TODO add a log message here.
			repo.logger.Info("Ignore the data without measurement key")
			continue
		}

		t := time.Unix(0, m.Created)

		// TODO: do we need add thing id to the tag if the publisher is not
		// the thing id of the device?
		// if there eorr of getting meta, ignore it
		// still save it to influx db
		var meta things.Metadata

		thingID := flat["thingId"]
		if thingID != nil {
			meta, _ = repo.thingsService.GetThingMetaById(thingID.(string))
		} else {
			meta, _ = repo.thingsService.GetThingMetaById(m.Publisher)
		}

		tgs := jsonTags(m, deviceName, meta)
		pt := influxdata.NewPoint(measurement, tgs, fields, t)
		repo.writeAPI.WritePoint(pt)
	}

	repo.writeAPI.Flush()

	return nil
}
