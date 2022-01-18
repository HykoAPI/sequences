package sequences

import (
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"testing"
	"time"
)

func TestPanicHandling(t *testing.T) {
	stage := &Stage{
		EventName:    "my-panicking-event",
		ConsumerFunc: func(db *gorm.DB, input []byte) (Status, string, *time.Time) {
			panic("I am  panicking")
			return SUCCESS, "", nil
		},
		NextStage:    nil,
	}

	ranStoreFunc := false

	consumer := Consumer{
		db:        nil,
		sequence:  Sequence{Stages: stage},
		taskQueue: nil,
		storeFunc: func(db *gorm.DB, ID uint, stage string, status Status, errorMessage string) error {
			ranStoreFunc = true
			require.Equal(t, errorMessage, "I am  panicking")
			return nil
		},
		readFunc: func(db *gorm.DB, ID uint, stage string) (bool, string, string, error) {
			return false, "", "", nil
		},
		logger:    zerolog.Logger{},
	}

	consumer.processEvent(nil, stage, Event{},nil)
	require.True(t, ranStoreFunc)
}
