package victoriametrics

import (
	"bytes"
	"github.com/loposkin/tsbs/pkg/targets"
	"log"
	"net/http"
	"time"
)

type processor struct {
	url    string
	vmURLs []string
}

func (p *processor) Init(workerNum int, doLoad, hashWorkers bool) {
	p.url = p.vmURLs[workerNum%len(p.vmURLs)]
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (float64, uint64, uint64) {
	batch := b.(*batch)
	if !doLoad {
		return 0, batch.metrics, batch.rows
	}
	lag, mc, rc := p.do(batch)

	return lag, mc, rc
}


func (p *processor) do(b *batch) (float64, uint64, uint64) {
	for {
		r := bytes.NewReader(b.buf.Bytes())
		req, err := http.NewRequest("POST", p.url, r)
		if err != nil {
			log.Fatalf("error while creating new request: %s", err)
		}
		start := time.Now()
		resp, err := http.DefaultClient.Do(req)

		if err != nil {
			log.Fatalf("error while executing request: %s", err)
		}
		resp.Body.Close()
		lag := float64(time.Since(start).Nanoseconds()) / 1e6 // milliseconds
		if resp.StatusCode == http.StatusNoContent {
			b.buf.Reset()
			return lag, b.metrics, b.rows
		}
		log.Printf("server returned HTTP status %d. Retrying", resp.StatusCode)
		time.Sleep(time.Millisecond * 10)
	}
}
