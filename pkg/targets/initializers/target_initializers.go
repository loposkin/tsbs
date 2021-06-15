package initializers

import (
	"fmt"
	"github.com/loposkin/tsbs/pkg/targets"
	"github.com/loposkin/tsbs/pkg/targets/akumuli"
	"github.com/loposkin/tsbs/pkg/targets/cassandra"
	"github.com/loposkin/tsbs/pkg/targets/clickhouse"
	"github.com/loposkin/tsbs/pkg/targets/constants"
	"github.com/loposkin/tsbs/pkg/targets/crate"
	"github.com/loposkin/tsbs/pkg/targets/influx"
	"github.com/loposkin/tsbs/pkg/targets/mongo"
	"github.com/loposkin/tsbs/pkg/targets/prometheus"
	"github.com/loposkin/tsbs/pkg/targets/siridb"
	"github.com/loposkin/tsbs/pkg/targets/timescaledb"
	"github.com/loposkin/tsbs/pkg/targets/timestream"
	"github.com/loposkin/tsbs/pkg/targets/victoriametrics"
	"strings"
)

func GetTarget(format string) targets.ImplementedTarget {
	switch format {
	case constants.FormatTimescaleDB:
		return timescaledb.NewTarget()
	case constants.FormatAkumuli:
		return akumuli.NewTarget()
	case constants.FormatCassandra:
		return cassandra.NewTarget()
	case constants.FormatClickhouse:
		return clickhouse.NewTarget()
	case constants.FormatCrateDB:
		return crate.NewTarget()
	case constants.FormatInflux:
		return influx.NewTarget()
	case constants.FormatMongo:
		return mongo.NewTarget()
	case constants.FormatPrometheus:
		return prometheus.NewTarget()
	case constants.FormatSiriDB:
		return siridb.NewTarget()
	case constants.FormatVictoriaMetrics:
		return victoriametrics.NewTarget()
	case constants.FormatTimestream:
		return timestream.NewTarget()
	}

	supportedFormatsStr := strings.Join(constants.SupportedFormats(), ",")
	panic(fmt.Sprintf("Unrecognized format %s, supported: %s", format, supportedFormatsStr))
}
