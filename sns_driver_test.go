package es_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/indebted-modules/es"
	"github.com/stretchr/testify/suite"
)

type SNSNotifierSuite struct {
	suite.Suite
	snsSvc   *sns.SNS
	sqsSvc   *sqs.SQS
	topicArn *string
	queueURL *string
}

func TestSNSNotifierSuite(t *testing.T) {
	suite.Run(t, new(SNSNotifierSuite))
}

func (s *SNSNotifierSuite) SetupSuite() {
	snsEndpoint := "http://localstack:4575"
	sqsEndpoint := "http://localstack:4576"
	s.Eventually(func() bool {
		_, err1 := http.Get(snsEndpoint)
		_, err2 := http.Get(sqsEndpoint)
		return err1 == nil && err2 == nil
	}, 10*time.Second, time.Second, "Localstack services are not ready or running")

	sess := session.Must(session.NewSession())
	cred := credentials.NewStaticCredentials("id", "secret", "token")
	s.snsSvc = sns.New(sess, aws.NewConfig().WithRegion("ap-southeast-2").WithCredentials(cred).WithEndpoint(snsEndpoint))
	topicResponse, err := s.snsSvc.CreateTopic(&sns.CreateTopicInput{Name: aws.String("test-topic")})
	s.NoError(err)
	s.topicArn = topicResponse.TopicArn

	s.sqsSvc = sqs.New(sess, aws.NewConfig().WithRegion("ap-southeast-2").WithCredentials(cred).WithEndpoint(sqsEndpoint))
	queueResponse, err := s.sqsSvc.CreateQueue(&sqs.CreateQueueInput{QueueName: aws.String("test-queue")})
	s.NoError(err)
	s.queueURL = queueResponse.QueueUrl

	_, err = s.snsSvc.Subscribe(&sns.SubscribeInput{
		Protocol: aws.String("sqs"),
		Endpoint: queueResponse.QueueUrl,
		TopicArn: topicResponse.TopicArn,
	})
	s.NoError(err)
}

func (s *SNSNotifierSuite) TearDownSuite() {
	_, err := s.sqsSvc.DeleteQueue(&sqs.DeleteQueueInput{QueueUrl: s.queueURL})
	s.NoError(err)
	_, err = s.snsSvc.DeleteTopic(&sns.DeleteTopicInput{TopicArn: s.topicArn})
	s.NoError(err)
}

func (s *SNSNotifierSuite) TestDelegateLoadToInternalDriver() {
	inMemoryDriver := es.NewInMemoryDriver()
	err := inMemoryDriver.Save([]*es.Event{es.NewEvent("123", &SomethingHappened{})})
	s.NoError(err)

	driver := es.NewSNSDriver(s.snsSvc, *s.topicArn, inMemoryDriver)

	events, err := driver.Load("123")
	s.NoError(err)
	s.Equal(&SomethingHappened{}, events[0].Payload)
}

func (s *SNSNotifierSuite) TestSaveDoesNotPublishWhenBrokenDriver() {
	brokenDriver := &BrokenDriver{ErrorMessage: "borked!"}
	driver := es.NewSNSDriver(s.snsSvc, *s.topicArn, brokenDriver)

	err := driver.Save([]*es.Event{})
	s.Error(err, "borked!")

	response, err := s.sqsSvc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:        s.queueURL,
		WaitTimeSeconds: aws.Int64(1),
	})
	s.NoError(err)
	s.Equal(0, len(response.Messages))
}

func (s *SNSNotifierSuite) TestSaveDoesNotPublishWhenNoEvents() {
	inMemoryDriver := es.NewInMemoryDriver()
	driver := es.NewSNSDriver(s.snsSvc, *s.topicArn, inMemoryDriver)

	err := driver.Save([]*es.Event{})
	s.NoError(err)

	response, err := s.sqsSvc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:        s.queueURL,
		WaitTimeSeconds: aws.Int64(1),
	})
	s.NoError(err)
	s.Equal(0, len(response.Messages))
}

func (s *SNSNotifierSuite) TestPublishesOnceAndDeduplicatedEventTypes() {
	driver := es.NewSNSDriver(s.snsSvc, *s.topicArn, es.NewInMemoryDriver())
	err := driver.Save([]*es.Event{
		es.NewEvent("uuid-1", &SomethingHappened{}),
		es.NewEvent("uuid-2", &SomethingElseHappened{}),
		es.NewEvent("uuid-3", &SomethingHappened{}),
		es.NewEvent("uuid-4", &SomethingElseHappened{}),
	})
	s.NoError(err)

	response, err := s.sqsSvc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:        s.queueURL,
		WaitTimeSeconds: aws.Int64(1),
	})
	s.NoError(err)
	s.Equal(1, len(response.Messages))

	body := &struct {
		Message           string
		MessageAttributes map[string]map[string]interface{}
	}{}

	err = json.Unmarshal([]byte(*response.Messages[0].Body), body)
	s.NoError(err)
	s.JSONEq(`{
		"SomethingHappened": ["1","3"],
		"SomethingElseHappened": ["2","4"]
	}`, body.Message)
	s.Equal("String.Array", body.MessageAttributes["EventTypes"]["Type"])
	s.Equal(`["SomethingHappened","SomethingElseHappened"]`, body.MessageAttributes["EventTypes"]["Value"])
}

func (s *SNSNotifierSuite) TestDelegateReadEventsForwardToInternalDriver() {
	inMemoryDriver := es.NewInMemoryDriver()
	err := inMemoryDriver.Save([]*es.Event{es.NewEvent("123", &SomethingHappened{})})
	s.NoError(err)

	driver := es.NewSNSDriver(s.snsSvc, *s.topicArn, inMemoryDriver)

	events, err := driver.ReadEventsOfTypes(0, 1)
	s.NoError(err)
	s.Equal(&SomethingHappened{}, events[0].Payload)
}
