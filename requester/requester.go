// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package requester provides commands to run load tests and display results.
package requester

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"
)

// Max size of the buffer of result channel.
const maxResult = 1000000
const maxIdleConn = 500

type result struct {
	err           error
	statusCode    int
	offset        time.Duration
	duration      time.Duration
	connDuration  time.Duration // connection setup(DNS lookup + Dial up) duration
	dnsDuration   time.Duration // dns lookup duration
	reqDuration   time.Duration // request "write" duration
	resDuration   time.Duration // response "read" duration
	delayDuration time.Duration // delay between response and request
	contentLength int64
}

type Work struct {
	// Request is the request to be made.
	Request *http.Request

	RequestBody []byte

	// RequestFunc is a function to generate requests. If it is nil, then
	// Request and RequestData are cloned for each request.
	RequestFunc func() *http.Request

	// N is the total number of requests to make.
	N int

	// C is the concurrency level, the number of concurrent workers to run.
	C int

	// H2 is an option to make HTTP/2 requests
	H2 bool

	// Timeout in seconds.
	Timeout int

	RampupDuration time.Duration

	RampupStepCount int

	// Qps is the rate limit in queries per second.
	QPS float64

	// DisableCompression is an option to disable compression in response
	DisableCompression bool

	// DisableKeepAlives is an option to prevents re-use of TCP connections between different HTTP requests
	DisableKeepAlives bool

	// DisableRedirects is an option to prevent the following of HTTP redirects
	DisableRedirects bool

	Debug bool
	// Output represents the output type. If "csv" is provided, the
	// output will be dumped as a csv stream.
	Output string

	// ProxyAddr is the address of HTTP proxy server in the format on "host:port".
	// Optional.
	ProxyAddr *url.URL

	// Writer is where results will be written. If nil, results are written to stdout.
	Writer io.Writer

	initOnce  sync.Once
	results   chan *result
	stopCh    chan struct{}
	start     time.Duration
	workersCh chan struct{}

	report *report
}

func (b *Work) writer() io.Writer {
	if b.Writer == nil {
		return os.Stdout
	}
	return b.Writer
}

// Init initializes internal data-structures
func (b *Work) Init() {
	b.initOnce.Do(func() {
		b.results = make(chan *result, min(b.C*1000, maxResult))
		b.stopCh = make(chan struct{}, b.C)
		b.workersCh = make(chan struct{}, b.C)
	})
}

// Run makes all the requests, prints the summary. It blocks until
// all work is done.
func (b *Work) Run() {
	b.Init()
	b.start = now()
	b.report = newReport(b.writer(), b.results, b.Output, b.N)
	// Run the reporter first, it polls the result channel until it is closed.
	go func() {
		runReporter(b.report)
	}()
	b.runWorkers()
	b.Finish()
}

func (b *Work) Stop() {
	// Send stop signal so that workers can stop gracefully.
	close(b.stopCh)
}

func (b *Work) Finish() {
	close(b.results)
	total := now() - b.start
	// Wait until the reporter is done.
	<-b.report.done
	b.report.finalize(total)
}

func (b *Work) makeRequest(stopCh chan struct{}, c *http.Client) {
	s := now()
	var size int64
	var code int
	var dnsStart, connStart, resStart, reqStart, delayStart time.Duration
	var dnsDuration, connDuration, resDuration, reqDuration, delayDuration time.Duration
	var req *http.Request
	if b.RequestFunc != nil {
		req = b.RequestFunc()
		if req == nil {
			return
		}
	} else {
		req = cloneRequest(b.Request, b.RequestBody)
	}
	trace := &httptrace.ClientTrace{
		DNSStart: func(info httptrace.DNSStartInfo) {
			dnsStart = now()
		},
		DNSDone: func(dnsInfo httptrace.DNSDoneInfo) {
			dnsDuration = now() - dnsStart
		},
		GetConn: func(h string) {
			connStart = now()
		},
		GotConn: func(connInfo httptrace.GotConnInfo) {
			if !connInfo.Reused {
				connDuration = now() - connStart
			}
			reqStart = now()
		},
		WroteRequest: func(w httptrace.WroteRequestInfo) {
			reqDuration = now() - reqStart
			delayStart = now()
		},
		GotFirstResponseByte: func() {
			delayDuration = now() - delayStart
			resStart = now()
		},
	}
	// ctx, cancel := context.WithCancel(httptrace.WithClientTrace(req.Context(), trace))
	// httptrace.WithClientTrace(req.Context(), trace)
	// ctx, cancel := context.WithCancel(httptrace.WithClientTrace(req.Context(), trace))
	// req = req.WithContext(ctx)
	// doneCh := make(chan struct{})
	// defer close(doneCh)

	// go func() {
	// 	select {
	// 	case <-stopCh:
	// 		cancel()
	// 	case <-doneCh:
	// 		return
	// 	}
	// }()

	if b.Debug {
		printRequest(req)
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	resp, err := c.Do(req)
	if err == nil {
		size = resp.ContentLength
		code = resp.StatusCode
		if b.Debug {
			printResponse(resp)
		} else {
			io.Copy(ioutil.Discard, resp.Body)
		}
		resp.Body.Close()
	}
	t := now()
	resDuration = t - resStart
	finish := t - s
	b.results <- &result{
		offset:        s,
		statusCode:    code,
		duration:      finish,
		err:           err,
		contentLength: size,
		connDuration:  connDuration,
		dnsDuration:   dnsDuration,
		reqDuration:   reqDuration,
		resDuration:   resDuration,
		delayDuration: delayDuration,
	}
}

func (b *Work) runWorker(client *http.Client, n int) {
	var throttle <-chan time.Time
	if b.QPS > 0 {
		throttle = time.Tick(time.Duration(1e6/(b.QPS)) * time.Microsecond)
	}

	if b.DisableRedirects {
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}
	for i := 0; i < n; i++ {
		// Check if application is stopped. Do not send into a closed channel.
		select {
		case <-b.stopCh:
			return
		default:
			if b.QPS > 0 {
				<-throttle
			}
			b.makeRequest(b.stopCh, client)
		}
	}
}

func (b *Work) createWorkerCh(n int) {
	for i := 0; i < n; i++ {
		b.workersCh <- struct{}{}
	}
}
func (b *Work) createWorkers() {
	if b.RampupDuration <= 0 {
		b.createWorkerCh(b.C)
		close(b.workersCh)
		return
	}

	b.createRampupWorkers()
}

func (b *Work) createRampupWorkers() {
	if b.RampupStepCount > 0 {
		count := b.C / b.RampupStepCount
		duration := time.Duration(int(b.RampupDuration) / b.RampupStepCount)
		for i := 0; i < b.RampupStepCount; i++ {
			if i == b.RampupStepCount-1 {
				count += b.C % b.RampupStepCount
			}
			b.createWorkerCh(count)
			time.Sleep(duration)
		}
	} else {
		// linear ramp-up
		duration := time.Duration(int(b.RampupDuration) / b.C)
		for i := 0; i < b.C; i++ {
			b.createWorkerCh(1)
			time.Sleep(duration)
		}
	}
}

func (b *Work) runWorkers() {
	var wg sync.WaitGroup

	var serverName string
	if b.Request != nil {
		serverName = b.Request.Host
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         serverName,
		},
		MaxIdleConnsPerHost: min(b.C, maxIdleConn),
		DisableCompression:  b.DisableCompression,
		DisableKeepAlives:   b.DisableKeepAlives,
		Proxy:               http.ProxyURL(b.ProxyAddr),
	}
	if b.H2 {
		http2.ConfigureTransport(tr)
	} else {
		tr.TLSNextProto = make(map[string]func(string, *tls.Conn) http.RoundTripper)
	}
	client := &http.Client{Transport: tr, Timeout: time.Duration(b.Timeout) * time.Second}

	go b.createWorkers()
Loop:
	for {
		select {
		case <-b.stopCh:
			break Loop
		case <-b.workersCh:
			wg.Add(1)
			go func() {
				b.runWorker(client, b.N/b.C)
				wg.Done()
			}()
		}
	}
	wg.Wait()
}

// cloneRequest returns a clone of the provided *http.Request.
// The clone is a shallow copy of the struct and its Header map.
func cloneRequest(r *http.Request, body []byte) *http.Request {
	// shallow copy of the struct
	r2 := new(http.Request)
	*r2 = *r
	// deep copy of the Header
	r2.Header = make(http.Header, len(r.Header))
	for k, s := range r.Header {
		r2.Header[k] = append([]string(nil), s...)
	}
	if len(body) > 0 {
		r2.Body = ioutil.NopCloser(bytes.NewReader(body))
	}
	return r2
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func printRequest(r *http.Request) {
	if r == nil {
		return
	}
	fmt.Println("> Method: ", r.Method)
	fmt.Println("> URL: ", r.URL)
	fmt.Println("> Proto: ", r.Proto)
	printHeader(r.Header)
	if r.Body != nil {
		body, _ := ioutil.ReadAll(r.Body)
		if len(body) > 0 {
			fmt.Println("> Body: ", string(body))
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
	}
	fmt.Println()
}

func printResponse(r *http.Response) {
	body, _ := ioutil.ReadAll(r.Body)
	fmt.Println(string(body))
}

func printHeader(h http.Header) {
	for k, v := range h {
		fmt.Printf("> %v: %v\n", k, strings.Join(v, ";"))
	}
}
