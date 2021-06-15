package mongo

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/loposkin/tsbs/pkg/data/serialize"
	"github.com/loposkin/tsbs/pkg/data/source"
	"github.com/loposkin/tsbs/pkg/targets"
	"github.com/loposkin/tsbs/pkg/targets/constants"
	"time"
)

func NewTarget() targets.ImplementedTarget {
	return &mongoTarget{}
}

type mongoTarget struct {
}

func (t *mongoTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"url", "localhost:27017", "Mongo URL.")
	flagSet.Duration(flagPrefix+"write-timeout", 10*time.Second, "Write timeout.")
	flagSet.Bool(flagPrefix+"document-per-event", false, "Whether to use one document per event or aggregate by hour")
}

func (t *mongoTarget) TargetName() string {
	return constants.FormatMongo
}

func (t *mongoTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *mongoTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}
