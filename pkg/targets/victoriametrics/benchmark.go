package victoriametrics

import (
	"bufio"
	"bytes"
	"errors"
	"github.com/blagojts/viper"
	"github.com/loposkin/tsbs/load"
	"github.com/loposkin/tsbs/pkg/data/source"
	"github.com/loposkin/tsbs/pkg/targets"
	"sync"
	"sync/atomic"
)

type SpecificConfig struct {
	ServerURLs []string `yaml:"urls" mapstructure:"urls"`
}

func parseSpecificConfig(v *viper.Viper) (*SpecificConfig, error) {
	var conf SpecificConfig
	if err := v.Unmarshal(&conf); err != nil {
		return nil, err
	}
	return &conf, nil
}

// loader.Benchmark interface implementation
type benchmark struct {
	serverURLs []string
	dataSource targets.DataSource
	butchCounter uint64
}

func NewBenchmark(vmSpecificConfig *SpecificConfig, dataSourceConfig *source.DataSourceConfig) (*benchmark, error) {
	if dataSourceConfig.Type != source.FileDataSourceType {
		return nil, errors.New("only FILE data source type is supported for VictoriaMetrics")
	}

	br := load.GetBufferedReader(dataSourceConfig.File.Location)
	return &benchmark{
		dataSource: &fileDataSource{
			scanner: bufio.NewScanner(br),
		},
		serverURLs: vmSpecificConfig.ServerURLs,
	}, nil
}


func (b *benchmark) GetDataSource() targets.DataSource {
	return b.dataSource
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	bufPool := sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 16*1024*1024))
		},
	}
	return &factory{bufPool: &bufPool, butchCounter: &b.butchCounter}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{vmURLs: b.serverURLs}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{}
}

type factory struct {
	bufPool *sync.Pool
	butchCounter *uint64
}

func (f *factory) New() targets.Batch {
	butchId := atomic.AddUint64(f.butchCounter, 1)
	return &batch{buf: f.bufPool.Get().(*bytes.Buffer), id: butchId}
}
