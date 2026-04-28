// Package client is a thin HTTP client for DistributedKV.
//
// It hides leader-redirects from the caller: any node can take a write
// and respond with 307 Location pointing at the leader. The client
// follows those automatically, with a small bounded retry budget.
package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// WatchEvent mirrors the server's published event shape — duplicated
// here to keep the client a leaf module with no dependency on internal/fsm.
type WatchEvent struct {
	Kind    string `json:"kind"`
	Key     string `json:"key"`
	Value   []byte `json:"value,omitempty"`
	Version uint64 `json:"version"`
	At      string `json:"at"`
}

const (
	defaultMaxRedirects = 4
	defaultTimeout      = 5 * time.Second
)

// Client speaks to a DistributedKV cluster.
type Client struct {
	endpoints []string
	hc        *http.Client
}

// New returns a client with the given seed endpoints. Order matters:
// the first endpoint is dialed first; on failure the rest are tried
// in order.
func New(endpoints ...string) *Client {
	hc := &http.Client{
		Timeout: defaultTimeout,
		// Disable automatic redirect-following so we can retain the
		// original method on POST/PUT redirects.
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return &Client{endpoints: endpoints, hc: hc}
}

// PutResult mirrors the server's write response shape.
type PutResult struct {
	Version   uint64  `json:"version"`
	ElapsedMS float64 `json:"elapsed_ms"`
}

// Put writes value to key, returning the new version.
func (c *Client) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	body := bytes.NewReader(value)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://x/v1/kv/"+key, body)
	if err != nil {
		return 0, err
	}
	resp, err := c.do(req, nil)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return 0, c.readErr(resp)
	}
	var r PutResult
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return 0, err
	}
	return r.Version, nil
}

// Get returns (value, version). Set stale=true for a fast follower
// read; set false (default) for a linearizable read.
func (c *Client) Get(ctx context.Context, key string, stale bool) ([]byte, uint64, error) {
	url := "http://x/v1/kv/" + key
	if stale {
		url += "?stale=1"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := c.do(req, nil)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, 0, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, 0, c.readErr(resp)
	}
	v, _ := strconv.ParseUint(resp.Header.Get("X-DKV-Version"), 10, 64)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}
	return body, v, nil
}

// Delete removes a key.
func (c *Client) Delete(ctx context.Context, key string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, "http://x/v1/kv/"+key, nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return c.readErr(resp)
	}
	return nil
}

// CAS performs a compare-and-swap. Returns (newVersion, conflictVersion, err).
// If the server returned a version_mismatch, err is nil and conflictVersion
// contains the actual on-disk version.
func (c *Client) CAS(ctx context.Context, key string, expected uint64, value []byte) (uint64, uint64, error) {
	payload, _ := json.Marshal(map[string]any{
		"expected_version": expected,
		"value":            string(value),
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://x/v1/kv/"+key+"/cas", bytes.NewReader(payload))
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req, nil)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		var r PutResult
		_ = json.NewDecoder(resp.Body).Decode(&r)
		return r.Version, 0, nil
	}
	if resp.StatusCode == http.StatusConflict {
		var r struct {
			ConflictVersion uint64 `json:"conflict_version"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&r)
		return 0, r.ConflictVersion, nil
	}
	return 0, 0, c.readErr(resp)
}

// Watch streams write events for keys whose name begins with `prefix`.
// Events are pushed onto out; the channel is closed when ctx is cancelled
// or the server drops the subscriber. Returns the first endpoint that
// answered the SSE handshake, plus an error if no endpoint did.
//
// The channel is buffered (cap=64); a slow consumer that doesn't read
// will see the server side mark them as dropped and close the stream.
func (c *Client) Watch(ctx context.Context, prefix string, out chan<- WatchEvent) error {
	defer close(out)
	hc := &http.Client{}  // separate client — no Timeout, since this is long-poll
	for _, ep := range c.endpoints {
		url := fmt.Sprintf("http://%s/v1/watch?prefix=%s", ep, prefix)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := hc.Do(req)
		if err != nil {
			continue  // try next endpoint
		}
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			continue
		}
		// Parse the SSE stream. Frames are separated by "\n\n"; each
		// frame has lines like "event: NAME" and "data: {...}".
		defer resp.Body.Close()
		return parseSSE(resp.Body, out)
	}
	return ErrNoEndpoints
}

func parseSSE(r io.Reader, out chan<- WatchEvent) error {
	br := bufio.NewReaderSize(r, 64*1024)
	var event, data string
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			// Frame boundary; emit if we have a payload.
			if event != "" && data != "" && event != "hello" && event != "dropped" {
				var e WatchEvent
				if err := json.Unmarshal([]byte(data), &e); err == nil {
					out <- e
				}
			}
			event, data = "", ""
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue  // heartbeat / comment
		}
		if strings.HasPrefix(line, "event:") {
			event = strings.TrimSpace(line[6:])
		} else if strings.HasPrefix(line, "data:") {
			data = strings.TrimSpace(line[5:])
		}
	}
}

// ClusterInfo returns the cluster topology + leader.
func (c *Client) ClusterInfo(ctx context.Context) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://x/v1/cluster", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, c.readErr(resp)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// do sends req against the seed endpoints, following any 307 redirects
// to the leader up to defaultMaxRedirects times.
func (c *Client) do(req *http.Request, body []byte) (*http.Response, error) {
	var lastErr error
	for _, ep := range c.endpoints {
		req.URL.Host = ep
		req.URL.Scheme = "http"
		req.Host = ep
		// We may need to retry the body across redirects, so capture it.
		if req.Body != nil && body == nil {
			b, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			body = b
			req.Body = io.NopCloser(bytes.NewReader(b))
		}
		for redirects := 0; redirects < defaultMaxRedirects; redirects++ {
			resp, err := c.hc.Do(req)
			if err != nil {
				lastErr = err
				break // try next endpoint
			}
			if resp.StatusCode != http.StatusTemporaryRedirect {
				return resp, nil
			}
			loc := resp.Header.Get("Location")
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if loc == "" {
				return nil, fmt.Errorf("307 with no Location header")
			}
			next, err := http.NewRequestWithContext(req.Context(), req.Method, loc, bytes.NewReader(body))
			if err != nil {
				return nil, err
			}
			next.Header = req.Header.Clone()
			req = next
		}
	}
	if lastErr == nil {
		lastErr = ErrNoEndpoints
	}
	return nil, lastErr
}

func (c *Client) readErr(resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("dkv: %d %s — %s", resp.StatusCode, resp.Status, string(b))
}

// --- errors -----------------------------------------------------------

type clientError string

func (e clientError) Error() string { return string(e) }

const (
	ErrNotFound    = clientError("dkv: not found")
	ErrNoEndpoints = clientError("dkv: all endpoints failed")
)
