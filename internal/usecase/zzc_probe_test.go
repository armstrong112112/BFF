package usecase

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ikermy/BFF/internal/domain"
)

// ─────────────────────────────────────────────────────────────────────────────
// Fakes for section C probes
// ─────────────────────────────────────────────────────────────────────────────

type cBilling struct {
	blocked   int
	blockedBy domain.QuoteBreakdown
	captured  int
	released  int
	captureN  []int
	releaseN  []int
	quote     domain.QuoteResult
}

func (b *cBilling) Quote(_ context.Context, _ string, units int, _ string) (domain.QuoteResult, error) {
	q := b.quote
	q.Requested = units
	return q, nil
}
func (b *cBilling) Block(_ context.Context, r domain.BlockRequest) error {
	b.blocked += r.Units
	b.blockedBy = r.BySource
	return nil
}
func (b *cBilling) Capture(_ context.Context, _ string, units int) error {
	b.captured += units
	b.captureN = append(b.captureN, units)
	return nil
}
func (b *cBilling) Release(_ context.Context, _ string, units int) error {
	b.released += units
	b.releaseN = append(b.releaseN, units)
	return nil
}
func (b *cBilling) GetBalance(_ context.Context, _ string) (domain.Balance, error) {
	return domain.Balance{}, nil
}
func (b *cBilling) TopUp(_ context.Context, _ string, _ float64) error { return nil }

// barcode client that fails the first N generate calls
type cBarcode struct {
	failFirst int
	calls     int
	genErr    error
}

func (c *cBarcode) Generate(_ context.Context, _ string, _ map[string]any) (domain.BarcodeItem, error) {
	return domain.BarcodeItem{URL: "u"}, nil
}
func (c *cBarcode) Calculate(_ context.Context, _, field string, _ map[string]any) (any, error) {
	return "calc-" + field, nil
}
func (c *cBarcode) Random(_ context.Context, _, field string, _ map[string]any) (any, error) {
	return "rand-" + field, nil
}
func (c *cBarcode) GeneratePDF417(_ context.Context, _ domain.GeneratePDF417Request) (domain.GeneratePDF417Response, error) {
	c.calls++
	if c.calls <= c.failFirst {
		if c.genErr != nil {
			return domain.GeneratePDF417Response{}, c.genErr
		}
		return domain.GeneratePDF417Response{}, errors.New("barcodegen: status 400: bad field")
	}
	return domain.GeneratePDF417Response{BarcodeURL: fmt.Sprintf("url-%d", c.calls), Format: "png"}, nil
}
func (c *cBarcode) GenerateCode128(_ context.Context, _ domain.GenerateCode128Request) (domain.GenerateCode128Response, error) {
	return domain.GenerateCode128Response{BarcodeURL: "c128"}, nil
}
func (c *cBarcode) GenerateRaw(_ context.Context, _ domain.GenerateRawRequest) (domain.GenerateRawResponse, error) {
	return domain.GenerateRawResponse{}, nil
}

type cEvents struct {
	generated []domain.BarcodeGeneratedEvent
	partial   []domain.PartialCompletedEvent
	completed int
}

func (e *cEvents) PublishBarcodeGenerated(_ context.Context, ev domain.BarcodeGeneratedEvent) error {
	e.generated = append(e.generated, ev)
	return nil
}
func (e *cEvents) PublishBarcodeEdited(_ context.Context, _ domain.BarcodeEditedEvent) error {
	return nil
}
func (e *cEvents) PublishPartialCompleted(_ context.Context, ev domain.PartialCompletedEvent) error {
	e.partial = append(e.partial, ev)
	return nil
}
func (e *cEvents) PublishSagaCompleted(_ context.Context, _ string) error {
	e.completed++
	return nil
}

type cRevStore struct{ cfg domain.RevisionConfig }

func (s *cRevStore) GetConfig(_ context.Context, _ string) (domain.RevisionConfig, error) {
	return s.cfg, nil
}
func (s *cRevStore) SaveConfig(_ context.Context, _ domain.RevisionConfig) error { return nil }
func (s *cRevStore) ListConfigs(_ context.Context) ([]domain.RevisionConfig, error) {
	return []domain.RevisionConfig{s.cfg}, nil
}

type cAI struct {
	sigCalls   int
	photoCalls int
	lastName   string
}

func (a *cAI) GenerateSignature(_ context.Context, r domain.AISignatureRequest) (domain.AISignatureResponse, error) {
	a.sigCalls++
	a.lastName = r.FullName
	return domain.AISignatureResponse{ImageURL: "sig.png"}, nil
}
func (a *cAI) GeneratePhoto(_ context.Context, _ domain.AIPhotoRequest) (domain.AIPhotoResponse, error) {
	a.photoCalls++
	return domain.AIPhotoResponse{ImageURL: "photo.png"}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// C-01: Block uses full bySource breakdown even when generateCount < units
// ─────────────────────────────────────────────────────────────────────────────

func TestProbeC01_BlockBySourceMismatchOnPartial(t *testing.T) {
	b := &cBilling{quote: domain.QuoteResult{
		CanProcess:   true,
		Partial:      true,
		AllowedTotal: 3, // only 3 of 10 affordable
		UnitPrice:    2.0,
		BySource: domain.QuoteBreakdown{
			Subscription: domain.SourceBreakdown{Units: 1, Amount: 2},
			Credits:      domain.SourceBreakdown{Units: 1, Amount: 2},
			Wallet:       domain.SourceBreakdown{Units: 1, Amount: 2},
		},
	}}
	bc := &cBarcode{}
	ev := &cEvents{}
	uc := NewGenerateUseCase(b, bc, ev, NewQuoteUseCase(b))

	resp, err := uc.Execute(context.Background(), "u1", domain.GenerateRequest{
		Units: 10, Revision: "R", Confirmed: true, BarcodeType: "pdf417",
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	sum := b.blockedBy.Subscription.Units + b.blockedBy.Credits.Units + b.blockedBy.Wallet.Units
	t.Logf("PROBE C-01: requested=10 allowedTotal=3 blockedUnits=%d bySourceUnitsSum=%d barcodes=%d",
		b.blocked, sum, len(resp.Barcodes))
	t.Logf("PROBE C-01: Block.Units=%d but bySource sums to %d -> mismatch=%v",
		b.blocked, sum, b.blocked != sum)
}

// ─────────────────────────────────────────────────────────────────────────────
// C-02: partial quote -> Release is NOT called for un-generated units
// ─────────────────────────────────────────────────────────────────────────────

func TestProbeC02_PartialQuoteNoReleaseOfUnusedUnits(t *testing.T) {
	b := &cBilling{quote: domain.QuoteResult{
		CanProcess: true, Partial: true, AllowedTotal: 3, UnitPrice: 2.0,
		BySource: domain.QuoteBreakdown{Wallet: domain.SourceBreakdown{Units: 3, Amount: 6}},
	}}
	bc := &cBarcode{}
	ev := &cEvents{}
	uc := NewGenerateUseCase(b, bc, ev, NewQuoteUseCase(b))

	_, err := uc.Execute(context.Background(), "u1", domain.GenerateRequest{
		Units: 10, Revision: "R", Confirmed: true,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	t.Logf("PROBE C-02: blocked=%d captured=%d released=%d releaseCalls=%v",
		b.blocked, b.captured, b.released, b.releaseN)
	t.Logf("PROBE C-02: partial_completed events=%d releasedUnits field=%v",
		len(ev.partial), ev.partial)
}

// ─────────────────────────────────────────────────────────────────────────────
// C-03: totalCost divided by successCount in per-barcode event
// ─────────────────────────────────────────────────────────────────────────────

func TestProbeC03_PerBarcodeCostRounding(t *testing.T) {
	b := &cBilling{quote: domain.QuoteResult{
		CanProcess: true, AllowedTotal: 3, UnitPrice: 0.0, // force fallback path
		BySource: domain.QuoteBreakdown{
			Subscription: domain.SourceBreakdown{Units: 1, Amount: 1},
			Wallet:       domain.SourceBreakdown{Units: 2, Amount: 0.10},
		},
	}}
	bc := &cBarcode{}
	ev := &cEvents{}
	uc := NewGenerateUseCase(b, bc, ev, NewQuoteUseCase(b))

	resp, err := uc.Execute(context.Background(), "u1", domain.GenerateRequest{
		Units: 3, Revision: "R",
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	var sum float64
	for _, e := range ev.generated {
		sum += e.Billing.TotalCost
	}
	t.Logf("PROBE C-03: totalCost=%.10f events=%d perEvent=%.10f sumOfEvents=%.10f drift=%.10f",
		resp.Billing.TotalCost, len(ev.generated),
		ev.generated[0].Billing.TotalCost, sum, sum-resp.Billing.TotalCost)
}

// ─────────────────────────────────────────────────────────────────────────────
// C-04: retryable classification via string matching
// ─────────────────────────────────────────────────────────────────────────────

func TestProbeC04_RetryableStringMatching(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"real 500", errors.New("barcodegen: /gen: status 500: boom")},
		{"400 with body containing 'status 500'", errors.New("barcodegen: /gen: status 400: field status 500 invalid")},
		{"422 validation", errors.New("barcodegen: /gen: status 422: bad")},
		{"409 conflict", errors.New("barcodegen: /gen: status 409: dup")},
		{"429 rate limited", errors.New("barcodegen: /gen: status 429: slow down")},
		{"500 in user field echo", errors.New("barcodegen: /gen: status 400: invalid street '500 status 500 Ave'")},
	}
	for _, tc := range cases {
		t.Logf("PROBE C-04: %-45s retryable=%v", tc.name, isRetryableBarcodeGenError(tc.err))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// C-05: non-retryable error burns units without retry, counted as failed
// ─────────────────────────────────────────────────────────────────────────────

func TestProbeC05_ValidationErrorPerUnitRepeated(t *testing.T) {
	b := &cBilling{quote: domain.QuoteResult{
		CanProcess: true, AllowedTotal: 5, UnitPrice: 1,
		BySource: domain.QuoteBreakdown{Wallet: domain.SourceBreakdown{Units: 5, Amount: 5}},
	}}
	// every call fails with a 400 (non-retryable, deterministic)
	bc := &cBarcode{failFirst: 1000}
	ev := &cEvents{}
	uc := NewGenerateUseCase(b, bc, ev, NewQuoteUseCase(b))

	_, err := uc.Execute(context.Background(), "u1", domain.GenerateRequest{
		Units: 5, Revision: "R",
	})
	t.Logf("PROBE C-05: barcodeGenCalls=%d (units=5) err=%v", bc.calls, err)
	t.Logf("PROBE C-05: blocked=%d captured=%d released=%d", b.blocked, b.captured, b.released)
}

// ─────────────────────────────────────────────────────────────────────────────
// C-06: AI auto-trigger fires when signature not requested; nil-name formatting
// ─────────────────────────────────────────────────────────────────────────────

func TestProbeC06_AIAutoTriggerAndNilName(t *testing.T) {
	b := &cBilling{quote: domain.QuoteResult{
		CanProcess: true, AllowedTotal: 1, UnitPrice: 1,
		BySource: domain.QuoteBreakdown{Wallet: domain.SourceBreakdown{Units: 1, Amount: 1}},
	}}
	bc := &cBarcode{}
	ev := &cEvents{}
	ai := &cAI{}
	uc := NewGenerateUseCase(b, bc, ev, NewQuoteUseCase(b)).WithAI(ai)

	// client did NOT ask for a signature and provided NO name fields
	_, err := uc.Execute(context.Background(), "u1", domain.GenerateRequest{
		Units: 1, Revision: "R",
		GenerateSignature: false,
		Fields:            map[string]any{"eyeColor": "BRO"},
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	t.Logf("PROBE C-06: GenerateSignature=false -> ai.GenerateSignature calls=%d", ai.sigCalls)
	t.Logf("PROBE C-06: fullName passed to AI = %q", ai.lastName)
	t.Logf("PROBE C-06: signatureUrl injected into event fields = %v",
		ev.generated[0].Fields["signatureUrl"])
}

// ─────────────────────────────────────────────────────────────────────────────
// C-07: chain runs only when len(req.Fields) > 0
// ─────────────────────────────────────────────────────────────────────────────

func TestProbeC07_ChainSkippedWhenNoFields(t *testing.T) {
	cfg := domain.RevisionConfig{
		Name: "R", Enabled: true,
		CalculationChain: []domain.ChainEntry{
			{Field: "DAE", Source: "random"},
		},
	}
	b := &cBilling{quote: domain.QuoteResult{
		CanProcess: true, AllowedTotal: 1, UnitPrice: 1,
		BySource: domain.QuoteBreakdown{Wallet: domain.SourceBreakdown{Units: 1, Amount: 1}},
	}}
	bc := &cBarcode{}
	ev := &cEvents{}
	chain := NewChainExecutor(bc, &cRevStore{cfg: cfg})
	uc := NewGenerateUseCase(b, bc, ev, NewQuoteUseCase(b)).WithChainExecutor(chain)

	// no fields at all -> chain never runs, DAE never computed
	resp, err := uc.Execute(context.Background(), "u1", domain.GenerateRequest{
		Units: 1, Revision: "R", Fields: nil,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	t.Logf("PROBE C-07: Fields=nil -> computed=%v skipped=%v", resp.Computed, resp.Skipped)
	t.Logf("PROBE C-07: DAE present in event fields = %v",
		ev.generated[0].Fields["DAE"])

	// contrast: one field present -> chain runs
	ev2 := &cEvents{}
	uc2 := NewGenerateUseCase(b, bc, ev2, NewQuoteUseCase(b)).WithChainExecutor(chain)
	resp2, _ := uc2.Execute(context.Background(), "u1", domain.GenerateRequest{
		Units: 1, Revision: "R", Fields: map[string]any{"firstName": "Jo"},
	})
	t.Logf("PROBE C-07: Fields={firstName} -> computed=%v DAE=%v",
		resp2.Computed, ev2.generated[0].Fields["DAE"])
}

// ─────────────────────────────────────────────────────────────────────────────
// C-08: dependsOn checked against resolvedFields incl. computed; cycle/order
// ─────────────────────────────────────────────────────────────────────────────

func TestProbeC08_ChainOrderDependency(t *testing.T) {
	// DAK depends on DAE, but DAK is listed BEFORE DAE in the chain
	cfg := domain.RevisionConfig{
		Name: "R", Enabled: true,
		CalculationChain: []domain.ChainEntry{
			{Field: "DAK", Source: "calculate", DependsOn: []string{"DAE"}},
			{Field: "DAE", Source: "random"},
		},
	}
	chain := NewChainExecutor(&cBarcode{}, &cRevStore{cfg: cfg})
	_, err := chain.Execute(context.Background(), "R", map[string]any{"firstName": "Jo"})
	t.Logf("PROBE C-08: out-of-order chain -> err=%v", err)

	// self-dependency
	cfg2 := domain.RevisionConfig{
		Name: "R", Enabled: true,
		CalculationChain: []domain.ChainEntry{
			{Field: "X", Source: "calculate", DependsOn: []string{"X"}},
		},
	}
	chain2 := NewChainExecutor(&cBarcode{}, &cRevStore{cfg: cfg2})
	_, err2 := chain2.Execute(context.Background(), "R", map[string]any{"firstName": "Jo"})
	t.Logf("PROBE C-08: self-dependency -> err=%v", err2)
}

// ─────────────────────────────────────────────────────────────────────────────
// C-09: enabled=false revision still generates
// ─────────────────────────────────────────────────────────────────────────────

func TestProbeC09_DisabledRevisionStillGenerates(t *testing.T) {
	cfg := domain.RevisionConfig{
		Name: "R", Enabled: false, // DISABLED by admin
		RequiredInputFields: []string{"firstName"},
	}
	b := &cBilling{quote: domain.QuoteResult{
		CanProcess: true, AllowedTotal: 1, UnitPrice: 1,
		BySource: domain.QuoteBreakdown{Wallet: domain.SourceBreakdown{Units: 1, Amount: 1}},
	}}
	bc := &cBarcode{}
	ev := &cEvents{}
	uc := NewGenerateUseCase(b, bc, ev, NewQuoteUseCase(b)).
		WithRevisionStore(&cRevStore{cfg: cfg})

	resp, err := uc.Execute(context.Background(), "u1", domain.GenerateRequest{
		Units: 1, Revision: "R", Fields: map[string]any{"firstName": "Jo"},
	})
	t.Logf("PROBE C-09: revision enabled=false -> err=%v success=%v barcodes=%d captured=%d",
		err, resp.Success, len(resp.Barcodes), b.captured)
}

// ─────────────────────────────────────────────────────────────────────────────
// C-10: validateMinimumSet only runs when revisionStore != nil
// ─────────────────────────────────────────────────────────────────────────────

func TestProbeC10_NoValidationWithoutStore(t *testing.T) {
	b := &cBilling{quote: domain.QuoteResult{
		CanProcess: true, AllowedTotal: 1, UnitPrice: 1,
		BySource: domain.QuoteBreakdown{Wallet: domain.SourceBreakdown{Units: 1, Amount: 1}},
	}}
	bc := &cBarcode{}
	ev := &cEvents{}
	// no WithRevisionStore -> no minimum-set validation at all
	uc := NewGenerateUseCase(b, bc, ev, NewQuoteUseCase(b))

	_, err := uc.Execute(context.Background(), "u1", domain.GenerateRequest{
		Units: 1, Revision: "TOTALLY_UNKNOWN_REVISION", Fields: map[string]any{},
	})
	t.Logf("PROBE C-10: unknown revision + empty fields, no store -> err=%v captured=%d",
		err, b.captured)
}

// ─────────────────────────────────────────────────────────────────────────────
// C-11: referral eligibility uses quote wallet amount, not captured amount
// ─────────────────────────────────────────────────────────────────────────────

func TestProbeC11_ReferralOnPartialFailure(t *testing.T) {
	b := &cBilling{quote: domain.QuoteResult{
		CanProcess: true, AllowedTotal: 10, UnitPrice: 1,
		BySource: domain.QuoteBreakdown{Wallet: domain.SourceBreakdown{Units: 10, Amount: 10}},
	}}
	// 9 of 10 fail (non-retryable)
	bc := &cBarcode{failFirst: 9 * maxBarcodeGenRetries}
	ev := &cEvents{}
	uc := NewGenerateUseCase(b, bc, ev, NewQuoteUseCase(b))

	resp, err := uc.Execute(context.Background(), "u1", domain.GenerateRequest{
		Units: 10, Revision: "R",
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	t.Logf("PROBE C-11: success=%d captured=%d released=%d", len(resp.Barcodes), b.captured, b.released)
	t.Logf("PROBE C-11: totalCost=%.2f referralEligible=%.2f (wallet.Amount=%.2f for all 10)",
		resp.Billing.TotalCost, resp.Billing.ReferralEligible, b.quote.BySource.Wallet.Amount)
	t.Logf("PROBE C-11: response bySource still reports wallet.units=%d despite %d captured",
		resp.Billing.BySource.Wallet.Units, b.captured)
}
