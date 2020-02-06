package es

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/rs/zerolog/log"
)

// NewSNSDriver creates an SNSDriver
func NewSNSDriver(client *sns.SNS, topicArn string, driver Driver) *SNSDriver {
	return &SNSDriver{
		client:   client,
		topicArn: topicArn,
		driver:   driver,
	}
}

// SNSDriver creates a driver decorator that sends a notification to AWS' SNS
// when new events are saved.
type SNSDriver struct {
	client   *sns.SNS
	topicArn string
	driver   Driver
}

// Load delegates to internal driver
func (d *SNSDriver) Load(aggregateID string) ([]*Event, error) {
	return d.driver.Load(aggregateID)
}

// Save delegates to internal driver. If successful, it'll emit a single
// notification with all event types.
func (d *SNSDriver) Save(events []*Event) error {
	err := d.driver.Save(events)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}

	types, err := json.Marshal(d.extractEventTypes(events))
	if err != nil {
		log.
			Warn().
			Err(err).
			Msg("Failed marshaling the event types")

		return nil
	}

	_, err = d.client.Publish(&sns.PublishInput{
		TopicArn: aws.String(d.topicArn),
		Message:  aws.String("-"),
		MessageAttributes: map[string]*sns.MessageAttributeValue{
			"EventTypes": {
				DataType:    aws.String("String.Array"),
				StringValue: aws.String(string(types)),
			},
		},
	})
	if err != nil {
		log.
			Warn().
			Err(err).
			Msg("Failed publishing to SNS")

		return nil
	}

	return nil
}

func (d *SNSDriver) extractEventTypes(events []*Event) []string {
	set := make(map[string]bool)
	types := make([]string, 0, len(set))
	for _, event := range events {
		if set[event.Type] == false {
			set[event.Type] = true
			types = append(types, event.Type)
		}
	}
	return types
}
