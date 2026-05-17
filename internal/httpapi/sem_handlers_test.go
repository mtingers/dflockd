package httpapi

import (
	"io"
	"testing"
	"time"
)

// The 4 sem handlers — handleReleaseSem, handleRenewSem,
// handleEnqueueSem, handleWaitSem — sat at 0% coverage. The existing
// TestHTTP_Semaphore only exercised acquire + timeout. These tests
// pin the remaining endpoints' happy paths against the real router so
// a future refactor that breaks them is caught here, not in
// production.

func TestHTTP_SemRelease(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()

	c := newClient(t, base)
	c.startSession()

	resp := c.post("/v1/semaphores/sem-rel", semAcquireRequest{AcquireTimeoutS: 1, Limit: 1, LeaseTTLS: 30})
	var v opResponse
	decode(t, resp, &v)
	if v.Status != "ok" || v.Token == "" {
		t.Fatalf("acquire: %+v", v)
	}

	resp = c.post("/v1/semaphores/sem-rel/release", releaseRequest{Token: v.Token})
	if resp.StatusCode != 204 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("release: %d %s", resp.StatusCode, body)
	}
	resp.Body.Close()
}

func TestHTTP_SemRenew(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()

	c := newClient(t, base)
	c.startSession()

	resp := c.post("/v1/semaphores/sem-renew", semAcquireRequest{AcquireTimeoutS: 1, Limit: 1, LeaseTTLS: 30})
	var v opResponse
	decode(t, resp, &v)
	if v.Status != "ok" || v.Token == "" {
		t.Fatalf("acquire: %+v", v)
	}

	resp = c.post("/v1/semaphores/sem-renew/renew", renewRequest{Token: v.Token, LeaseTTLS: 90})
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("renew: %d %s", resp.StatusCode, body)
	}
	var rv renewResponse
	decode(t, resp, &rv)
	if rv.RemainingS != 90 {
		t.Fatalf("remaining %d, want 90", rv.RemainingS)
	}
}

// TestHTTP_SemEnqueueWait covers the semaphore two-phase flow:
// holder + queuer; queuer enqueues, queuer waits, holder releases,
// queuer's wait returns with the slot.
func TestHTTP_SemEnqueueWait(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()

	holder := newClient(t, base)
	holder.startSession()
	resp := holder.post("/v1/semaphores/sem-eq", semAcquireRequest{AcquireTimeoutS: 1, Limit: 1, LeaseTTLS: 30})
	var hv opResponse
	decode(t, resp, &hv)
	if hv.Status != "ok" {
		t.Fatalf("holder acquire: %+v", hv)
	}

	queuer := newClient(t, base)
	queuer.startSession()
	resp = queuer.post("/v1/semaphores/sem-eq/enqueue", semEnqueueRequest{Limit: 1, LeaseTTLS: 30})
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("enqueue: %d %s", resp.StatusCode, body)
	}
	var qv opResponse
	decode(t, resp, &qv)
	if qv.Status != "queued" {
		t.Fatalf("enqueue: %+v want queued", qv)
	}

	done := make(chan opResponse, 1)
	go func() {
		r := queuer.post("/v1/semaphores/sem-eq/wait", waitRequest{TimeoutS: 5})
		var v opResponse
		decode(t, r, &v)
		done <- v
	}()

	time.Sleep(50 * time.Millisecond)
	holder.post("/v1/semaphores/sem-eq/release", releaseRequest{Token: hv.Token}).Body.Close()

	select {
	case v := <-done:
		if v.Status != "ok" || v.Token == "" {
			t.Fatalf("wait result: %+v", v)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("wait did not return after release")
	}
}
