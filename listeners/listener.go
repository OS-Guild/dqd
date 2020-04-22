package listeners

import (
	"context"

	v1 "github.com/soluto/dqd/v1"
	"github.com/spf13/viper"
)

type Dispose func() error

type Listener interface {
	Add(source v1.Source, options *viper.Viper)
	Listen(context context.Context) error
}
