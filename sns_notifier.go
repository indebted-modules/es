package es

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
)

type Notifier interface {
	Publish(data interface{}) error
}

type snsNotifier struct {
	client   *sns.SNS
	topicArn string
}

func (p *snsNotifier) Publish(data interface{}) error {
	type message struct {
		Default string `json:"default"`
	}

	rawData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	rawMessage, err := json.Marshal(message{Default: string(rawData)})
	if err != nil {
		return err
	}

	_, err = p.client.Publish(&sns.PublishInput{
		Message:          aws.String(string(rawMessage)),
		MessageStructure: aws.String("json"),
		TopicArn:         aws.String(p.topicArn),
	})
	if err != nil {
		return err
	}

	return nil
}

func NewSNSNotifier(client *sns.SNS, topicArn string) Notifier {
	return &snsNotifier{
		client:   client,
		topicArn: topicArn,
	}
}
