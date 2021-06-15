package victoriametrics

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/loposkin/tsbs/pkg/targets"
	"log"
	"net/http"
	"sync"
	"time"
)

type processor struct {
	url    string
	vmURLs []string
	latenciesFile *bufio.Writer
	mu *sync.Mutex
}

func (p *processor) Init(workerNum int, doLoad, hashWorkers bool) {
	p.url = p.vmURLs[workerNum%len(p.vmURLs)]
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*batch)
	if !doLoad {
		return batch.metrics, batch.rows
	}
	took, mc, rc := p.do(batch)
	if p.latenciesFile != nil {
		p.writeLatency(batch, took)
	}
	return mc, rc
}

func (p *processor) writeLatency(b *batch, latency time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	line := fmt.Sprintf("%d %.3f\n", b.butchNumber, float64(latency.Microseconds())/1000)
	_, err := p.latenciesFile.WriteString(line)
	if err != nil {
		log.Fatalf("failed writing latencies to file: %s", err)
	}
}

func (p *processor) do(b *batch) (time.Duration, uint64, uint64) {
	for {
		r := bytes.NewReader(b.buf.Bytes())
		req, err := http.NewRequest("POST", p.url, r)
		if err != nil {
			log.Fatalf("error while creating new request: %s", err)
		}
		start := time.Now()
		resp, err := http.DefaultClient.Do(req)
		took := time.Since(start)
		if err != nil {
			log.Fatalf("error while executing request: %s", err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusNoContent {
			b.buf.Reset()
			return took, b.metrics, b.rows
		}
		log.Printf("server returned HTTP status %d. Retrying", resp.StatusCode)
		time.Sleep(time.Millisecond * 10)
	}
}
