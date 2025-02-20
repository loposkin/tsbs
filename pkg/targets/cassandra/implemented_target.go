package cassandra

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
	return &cassandraTarget{}
}

type cassandraTarget struct {
}

func (t *cassandraTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"hosts", "localhost:9042", "Comma separated list of Cassandra hosts in a cluster.")
	flagSet.Int(flagPrefix+"replication-factor", 1, "Number of nodes that must have a copy of each key.")
	flagSet.String(flagPrefix+"consistency", "ALL", "Desired write consistency level. See Cassandra consistency documentation. Default: ALL")
	flagSet.Duration(flagPrefix+"write-timeout", 10*time.Second, "Write timeout.")
}

func (t *cassandraTarget) TargetName() string {
	return constants.FormatCassandra
}

func (t *cassandraTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *cassandraTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}
