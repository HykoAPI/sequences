package sequences

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/adjust/rmq/v3"
	"gorm.io/gorm"
	"github.com/go-redis/redis/v7"
	"time"
)

// Construct tree of stages
// Emit first event
// Consume first event
// Call matching consumer
func SetupConsumersForSequence(db *gorm.DB, redisURL string, taskQueueName string, numberOfConsumersForSequence int, sequence Sequence, storeFunc StoreFunc, readFunc ReadFunc) (*rmq.Queue, error) {
	// TODO: Error channel
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(opt)

	connection, err := rmq.OpenConnectionWithRedisClient("", client, nil)
	if err != nil {
		return nil, err
	}

	taskQueue, err := connection.OpenQueue(taskQueueName)
	if err != nil {
		return nil, err
	}

	err = taskQueue.StartConsuming(10, time.Second)
	if err != nil {
		return nil, err
	}

	for i := 1; i <= numberOfConsumersForSequence; i++ {
		taskConsumer := &Consumer{
			db:        db,
			sequence:  sequence,
			taskQueue: taskQueue,
			storeFunc: storeFunc,
			readFunc: readFunc,
		}
		_, err = taskQueue.AddConsumer(fmt.Sprintf("task-consumer-%d", i), taskConsumer)
		if err != nil {
			return nil, err
		}

	}

	return &taskQueue, nil
}

type Event struct {
	EventType  string `json:"event_type"`
	SequenceID uint   `json:"sequence_id"`
	Payload    []byte `json:"input"`
	WaitUntil *time.Time `json:"wait_until"`
}

type StoreFunc func(db *gorm.DB, ID uint, stage string, status Status, errorMessage string) error
// TODO: Move to struct
type ReadFunc func(db *gorm.DB, ID uint, stage string) (bool, string, string, error)

type Consumer struct {
	db        *gorm.DB
	sequence  Sequence
	taskQueue rmq.Queue
	storeFunc StoreFunc
	readFunc  ReadFunc
}

func NewConsumer(db *gorm.DB, sequence Sequence, queue rmq.Queue, store StoreFunc, read ReadFunc) Consumer {
	return Consumer{
		db:        db,
		sequence:  sequence,
		taskQueue: queue,
		storeFunc: store,
		readFunc: read,
	}
}

func (consumer *Consumer) Consume(delivery rmq.Delivery) {
	var event Event
	if err := json.Unmarshal([]byte(delivery.Payload()), &event); err != nil {
		// handle json error
		if err := delivery.Reject(); err != nil {
			// handle reject error
			fmt.Println(err)
		}
		fmt.Println(err)
		return
	}
	
	if event.WaitUntil != nil {
		if time.Now().UTC().Before(*event.WaitUntil) {
			fmt.Println("REQUEUING", *event.WaitUntil, time.Now().UTC())
			// Requeue event
			// Emit same event
			taskBytes, err := json.Marshal(event)
			if err != nil {
				// DO NOT REJECT JUST WAIT UNTIL WE REPROCESS IT
				fmt.Println(err)
				return
			}

			err = consumer.taskQueue.PublishBytes(taskBytes)
			if err != nil {
				// handle error
				fmt.Println(err)
				return
			}

			// Then ack old one so that if we error on ack we'll at least reprocess both rather than neither
			if err := delivery.Ack(); err != nil {
				fmt.Println(err)
				return
			}
			return
		}
	}

	currentStage, err := consumer.findMatchingStage(event, delivery)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("ABOUT TO PROCESS EVENT")
	if err := consumer.processEvent(consumer.db, currentStage, event, delivery); err != nil {
		fmt.Println(err)
		return
	}
}

func (consumer *Consumer) emitNextEvent(currentStage *Stage, sequenceID uint, payload []byte, waitUntil *time.Time) error {
	if currentStage.NextStage == nil {
		return nil
	}

	nextTask := Event{
		SequenceID: sequenceID,
		EventType:  currentStage.NextStage.EventName,
		Payload:    payload,
		WaitUntil: waitUntil,
	}

	taskBytes, err := json.Marshal(nextTask)
	if err != nil {
		// handle error
		return err
	}

	err = consumer.taskQueue.PublishBytes(taskBytes)
	if err != nil {
		// handle error
		return err
	}
	return nil
}

func (consumer *Consumer) processEvent(db *gorm.DB, currentStage *Stage, event Event, delivery rmq.Delivery) error {
	fmt.Println("PROCESSING ", currentStage.EventName, event.EventType, event.WaitUntil)
	// Read stage
	exists, status2, _, err := consumer.readFunc(db ,event.SequenceID, event.EventType)
	if err != nil {
		// Not acking or rejecting because we want to retry this message
		return err
	}

	// If already run successfully and we're not set up to retry then emit next event
	if Status(status2) != RETRY && exists {
		if err := consumer.emitNextEvent(currentStage, event.SequenceID, event.Payload, nil); err != nil {
			return err
		}
		return nil
	}

	status, description, waitUntil := currentStage.ConsumerFunc(db, event.Payload)
	if status == ERROR {
		fmt.Println(description)
		err := consumer.storeFunc(db, event.SequenceID, currentStage.EventName, ERROR, description)
		if err != nil {
			fmt.Println(err)
			// Still reject msg on error
		}

		if err := delivery.Reject(); err != nil {
			return err
		}
		return err
	}

	err = consumer.storeFunc(db, event.SequenceID, currentStage.EventName, status, description)
	if err != nil {
		return err
	}
	if err := delivery.Ack(); err != nil {
		return err
	}

	if status == RETRY {
		// Requeue event
		// Emit same event
		event.WaitUntil = waitUntil
		taskBytes, err := json.Marshal(event)
		if err != nil {
			// DO NOT REJECT JUST WAIT UNTIL WE REPROCESS IT
			fmt.Println(err)
			return nil
		}

		err = consumer.taskQueue.PublishBytes(taskBytes)
		if err != nil {
			// handle error
			fmt.Println(err)
			return nil
		}

		// Then ack old one so that if we error on ack we'll at least reprocess both rather than neither
		if err := delivery.Ack(); err != nil {
			fmt.Println(err)
			return nil
		}
		return nil
	} else {
		if err := consumer.emitNextEvent(currentStage, event.SequenceID, event.Payload, nil); err != nil {
			return err
		}
	}

	return nil
}

func (consumer *Consumer) findMatchingStage(task Event, delivery rmq.Delivery) (*Stage, error) {
	currentStage := consumer.sequence.Stages
	for {
		if currentStage.EventName == task.EventType {
			// Found Stage return it
			return currentStage, nil
		}

		// If there if isn't next stage reject as can't find
		// appropriate stage
		if currentStage.NextStage == nil {
			if err := delivery.Reject(); err != nil {
				return nil, err
			}
			return nil, errors.New("no stage found")
		}

		currentStage = currentStage.NextStage
	}
}

type Status string
const (
	ERROR Status = "ERROR"
	SUCCESS Status = "SUCCESS"
	SKIPPING Status = "SKIPPING"
	RETRY Status = "RETRY"
)