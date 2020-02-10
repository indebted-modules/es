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

type snsMessage struct {
	eventTypes     []string
	eventIDsByType map[string][]string
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

	message := d.toSNSMessage(events)
	eventIDsByType, err := json.Marshal(message.eventIDsByType)
	if err != nil {
		log.
			Warn().
			Err(err).
			Msg("Failed marshaling eventIDsByType")

		return nil
	}

	eventTypes, err := json.Marshal(message.eventTypes)
	if err != nil {
		log.
			Warn().
			Err(err).
			Msg("Failed marshaling eventTypes")

		return nil
	}

	_, err = d.client.Publish(&sns.PublishInput{
		TopicArn: aws.String(d.topicArn),
		Message:  aws.String(string(eventIDsByType)),
		MessageAttributes: map[string]*sns.MessageAttributeValue{
			"EventTypes": {
				DataType:    aws.String("String.Array"),
				StringValue: aws.String(string(eventTypes)),
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

// ReadEventsOfTypes .
func (d *SNSDriver) ReadEventsOfTypes(position int64, count uint, types []string) ([]*Event, error) {
	return d.driver.ReadEventsOfTypes(position, count, []string{})
}

func (d *SNSDriver) toSNSMessage(events []*Event) *snsMessage {
	types := []string{}
	idsByType := map[string][]string{}
	for _, event := range events {
		if _, ok := idsByType[event.Type]; !ok {
			types = append(types, event.Type)
		}
		idsByType[event.Type] = append(idsByType[event.Type], event.ID)
	}
	return &snsMessage{
		eventTypes:     types,
		eventIDsByType: idsByType,
	}
}
