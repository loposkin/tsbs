package victoriametrics

import (
	"bufio"
	"bytes"
	"errors"
	"github.com/blagojts/viper"
	"github.com/prometheus/common/log"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"sync"
	"sync/atomic"
)

type SpecificConfig struct {
	ServerURLs []string `yaml:"urls" mapstructure:"urls"`
	LatenciesFile string `yaml:"latencies-file" mapstructure:"latencies-file"`
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
	latenciesFile *bufio.Writer
	dataSource targets.DataSource
	butchCounter uint64
}

func NewBenchmark(vmSpecificConfig *SpecificConfig, dataSourceConfig *source.DataSourceConfig) (*benchmark, error) {
	if dataSourceConfig.Type != source.FileDataSourceType {
		return nil, errors.New("only FILE data source type is supported for VictoriaMetrics")
	}

	br := load.GetBufferedReader(dataSourceConfig.File.Location)
	var bw *bufio.Writer
	if len(vmSpecificConfig.LatenciesFile) > 0 {
		bw = load.GetBufferedWriter(vmSpecificConfig.LatenciesFile)
	}
	return &benchmark{
		dataSource: &fileDataSource{
			scanner: bufio.NewScanner(br),
		},
		serverURLs: vmSpecificConfig.ServerURLs,
		latenciesFile: bw,
		butchCounter: 0,
	}, nil
}

func (b *benchmark) CloseLatenciesFile() {
	if b.latenciesFile != nil {
		err := b.latenciesFile.Flush()

		if err != nil {
			log.Fatalf("failed writing latencies to file: %s", err)
		}
	}
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
	return &processor{vmURLs: b.serverURLs, latenciesFile: b.latenciesFile}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{}
}

type factory struct {
	bufPool *sync.Pool
	butchCounter *uint64
}

func (f *factory) New() targets.Batch {
	butchNumber := atomic.AddUint64(f.butchCounter, 1)
	return &batch{buf: f.bufPool.Get().(*bytes.Buffer), butchNumber: butchNumber}
}
