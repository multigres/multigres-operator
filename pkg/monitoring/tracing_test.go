package monitoring

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/multigres/testkit/assert"
)

func TestStartReconcileSpan(t *testing.T) {
	c := assert.NewCollecting(t)
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	// Point the package-level Tracer at our test provider.
	Tracer = tp.Tracer(tracerName)

	ctx := context.Background()
	ctx, span := StartReconcileSpan(
		ctx,
		"MultigresCluster.Reconcile",
		"my-cluster",
		"default",
		"MultigresCluster",
	)
	span.End()

	spans := exporter.GetSpans()
	c.Require().Len(spans, 1, "expected 1 span, got %d", len(spans))

	s := spans[0]
	c.Eq("MultigresCluster.Reconcile", s.Name, "span name")

	wantAttrs := map[string]string{
		"k8s.resource.name": "my-cluster",
		"k8s.namespace":     "default",
		"k8s.resource.kind": "MultigresCluster",
	}
	for key, want := range wantAttrs {
		found := false
		for _, attr := range s.Attributes {
			if string(attr.Key) == key {
				found = true
				c.Eq(
					want,
					attr.Value.AsString(),
					"attribute %q = %q, want",
					key,
					attr.Value.AsString(),
				)
			}
		}
		c.True(found, "attribute %q not found on span", key)
	}

	// Verify the context carries the span.
	c.False(ctx == context.Background(), "expected context to carry span")
}

func TestStartChildSpan(t *testing.T) {
	c := assert.NewCollecting(t)
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	Tracer = tp.Tracer(tracerName)

	ctx := context.Background()
	ctx, parent := StartReconcileSpan(ctx, "Parent.Reconcile", "res", "ns", "Kind")
	_, child := StartChildSpan(ctx, "ChildOperation")
	child.End()
	parent.End()

	spans := exporter.GetSpans()
	c.Require().Len(spans, 2, "expected 2 spans, got %d", len(spans))

	// Child span should reference the parent's span context.
	childSpan := spans[0]
	parentSpan := spans[1]
	c.Eq(parentSpan.SpanContext.SpanID(), childSpan.Parent.SpanID(), "child parent span ID")
	c.Eq("ChildOperation", childSpan.Name, "child span name")
}

func TestRecordSpanError(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	Tracer = tp.Tracer(tracerName)

	t.Run("records error on span", func(t *testing.T) {
		c := assert.NewCollecting(t)
		exporter.Reset()
		_, span := StartReconcileSpan(context.Background(), "Op", "n", "ns", "K")
		testErr := errors.New("something failed")
		RecordSpanError(span, testErr)
		span.End()

		spans := exporter.GetSpans()
		c.Require().Len(spans, 1, "expected 1 span, got %d", len(spans))

		s := spans[0]
		c.Eq(codes.Error, s.Status.Code, "span status")
		c.Eq("something failed", s.Status.Description, "span status description")

		// Check that an error event was recorded.
		foundErrorEvent := false
		for _, event := range s.Events {
			if event.Name == "exception" {
				foundErrorEvent = true
				for _, attr := range event.Attributes {
					if attr.Key == attribute.Key("exception.message") &&
						attr.Value.AsString() == "something failed" {
						break
					}
				}
			}
		}
		c.True(foundErrorEvent, "expected an exception event on the span")
	})

	t.Run("nil error is no-op", func(t *testing.T) {
		c := assert.NewCollecting(t)
		exporter.Reset()
		_, span := StartReconcileSpan(context.Background(), "Op", "n", "ns", "K")
		RecordSpanError(span, nil)
		span.End()

		spans := exporter.GetSpans()
		c.Require().Len(spans, 1, "expected 1 span, got %d", len(spans))
		c.NotEq(codes.Error, spans[0].Status.Code, "nil error should not set error status")
	})
}

func TestInitTracing_NoopWhenEndpointUnset(t *testing.T) {
	// Ensure OTEL_EXPORTER_OTLP_ENDPOINT is unset.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	c := assert.NewAborting(t)

	shutdown, err := InitTracing(context.Background(), "test-svc", "v0.0.1")
	c.NoError(err, "InitTracing() returned error")
	c.NoError(shutdown(context.Background()), "shutdown() returned error")
}

func TestInjectAndExtractTraceContext(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	Tracer = tp.Tracer(tracerName)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	t.Run("round-trips trace context through annotations", func(t *testing.T) {
		c := assert.NewCollecting(t)
		ctx, span := Tracer.Start(context.Background(), "webhook")
		originalTraceID := span.SpanContext().TraceID()

		annotations := make(map[string]string)
		InjectTraceContext(ctx, annotations)
		span.End()

		if _, ok := annotations[annotationTraceparent]; !ok {
			t.Fatal("expected traceparent annotation to be set")
		}
		_, ok := annotations[annotationTraceparentTS]
		c.Require().True(ok, "expected traceparent-ts annotation to be set")

		parentCtx, isStale := ExtractTraceContext(annotations)
		c.False(isStale, "fresh annotation should not be stale")
		sc := trace.SpanFromContext(parentCtx).SpanContext()
		c.Eq(originalTraceID, sc.TraceID(), "extracted trace ID")
	})

	t.Run("stale annotation", func(t *testing.T) {
		ctx, span := Tracer.Start(context.Background(), "old-webhook")

		annotations := make(map[string]string)
		InjectTraceContext(ctx, annotations)
		span.End()

		// Backdate the timestamp by 15 minutes.
		staleTS := time.Now().Add(-15 * time.Minute).Unix()
		annotations[annotationTraceparentTS] = strconv.FormatInt(staleTS, 10)

		_, isStale := ExtractTraceContext(annotations)
		assert.NewCollecting(t).True(isStale, "expected stale annotation to be detected")
	})

	t.Run("missing annotation returns background context", func(t *testing.T) {
		c := assert.NewCollecting(t)
		parentCtx, isStale := ExtractTraceContext(map[string]string{})
		c.False(isStale, "empty annotations should not be stale")
		sc := trace.SpanFromContext(parentCtx).SpanContext()
		c.False(sc.IsValid(), "expected invalid span context from empty annotations")
	})

	t.Run("missing timestamp treated as stale", func(t *testing.T) {
		ctx, span := Tracer.Start(context.Background(), "no-ts-webhook")
		annotations := make(map[string]string)
		InjectTraceContext(ctx, annotations)
		span.End()

		delete(annotations, annotationTraceparentTS)

		_, isStale := ExtractTraceContext(annotations)
		assert.NewCollecting(t).True(isStale, "missing timestamp should be treated as stale")
	})
}

func TestEnrichLoggerWithTrace(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	Tracer = tp.Tracer(tracerName)

	t.Run("adds trace_id and span_id to logger", func(t *testing.T) {
		ctx, span := Tracer.Start(context.Background(), "test-op")
		defer span.End()

		// Set up a logger in context.
		ctx = logr.NewContext(ctx, logr.Discard())
		enrichedCtx := EnrichLoggerWithTrace(ctx)

		// The enriched context should have a logger that can extract values.
		logger := log.FromContext(enrichedCtx)
		// We can't easily inspect logr values, but we can verify the function
		// doesn't panic and returns a different context.
		assert.NewCollecting(t).
			False(enrichedCtx == ctx, "expected enriched context to differ from original")
		_ = logger
	})

	t.Run("noop for invalid span context", func(t *testing.T) {
		ctx := logr.NewContext(context.Background(), logr.Discard())
		result := EnrichLoggerWithTrace(ctx)
		// With no valid span, the context should be returned unchanged.
		assert.NewCollecting(t).False(result != ctx, "expected unchanged context for invalid span")
	})
}

func TestInitTracing_WithEndpoint(t *testing.T) {
	// Set the endpoint to trigger the real code path, and use the "none"
	// exporter so autoexport returns a noop exporter without network I/O.
	// This still exercises resource creation, provider setup, and global
	// tracer re-acquisition.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	c := assert.NewAborting(t)

	shutdown, err := InitTracing(context.Background(), "test-svc", "v0.0.1")
	c.NoError(err, "InitTracing() returned error")
	c.NotNil(shutdown, "expected non-nil shutdown function")
	c.NoError(shutdown(context.Background()), "shutdown() returned error")
}

func TestInitTracing_ExporterError(t *testing.T) {
	// Set endpoint to trigger exporter creation
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
	// Set invalid exporter type to trigger error in autoexport.NewSpanExporter
	t.Setenv("OTEL_TRACES_EXPORTER", "invalid-exporter-type")
	c := assert.NewCollecting(t)

	// InitTracing should fail
	shutdown, err := InitTracing(context.Background(), "test-svc", "v0.0.1")
	c.Require().Error(err, "InitTracing() should have failed with invalid exporter type")
	c.Require().Nil(shutdown, "shutdown function should be nil on error")
	c.StrContains(err.Error(), "creating OTLP exporter", "unexpected error message: %v", err)
}

func TestInitTracing_ResourceError(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	c := assert.NewCollecting(t)

	injectedErr := errors.New("synthetic resource failure")
	original := newResource
	newResource = func(ctx context.Context, opts ...resource.Option) (*resource.Resource, error) {
		return nil, injectedErr
	}
	t.Cleanup(func() { newResource = original })

	shutdown, err := InitTracing(context.Background(), "test-svc", "v0.0.1")
	c.Require().Error(err, "expected error from InitTracing when resource creation fails")
	c.StrContains(err.Error(), "creating OTel resource", "unexpected error message: %v", err)
	c.ErrorIs(err, injectedErr, "expected wrapped injectedErr, got")
	c.Require().Nil(shutdown, "shutdown function should be nil on error")
}

func TestInjectTraceContext_TracestateRename(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	Tracer = tp.Tracer(tracerName)

	// Use a composite propagator that injects both traceparent and tracestate.
	otel.SetTextMapPropagator(propagation.TraceContext{})

	ctx, span := Tracer.Start(context.Background(), "webhook")
	defer span.End()

	// Force a tracestate by setting it on the span.
	ts := trace.TraceState{}
	ts, _ = ts.Insert("vendor", "value")
	ctx = trace.ContextWithSpanContext(ctx, span.SpanContext().WithTraceState(ts))

	annotations := make(map[string]string)
	InjectTraceContext(ctx, annotations)

	// The standard "tracestate" key should be renamed.
	if _, ok := annotations["tracestate"]; ok {
		t.Error("standard 'tracestate' key should be renamed")
	}
	_, ok := annotations["multigres.com/tracestate"]
	assert.NewCollecting(t).True(ok, "expected 'multigres.com/tracestate' annotation to be set")
}

func TestExtractTraceContext_InvalidTimestamp(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	Tracer = tp.Tracer(tracerName)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	ctx, span := Tracer.Start(context.Background(), "webhook")
	annotations := make(map[string]string)
	InjectTraceContext(ctx, annotations)
	span.End()

	// Set an invalid (non-numeric) timestamp.
	annotations[annotationTraceparentTS] = "not-a-number"

	_, isStale := ExtractTraceContext(annotations)
	assert.NewCollecting(t).True(isStale, "invalid timestamp should be treated as stale")
}

func TestInjectTraceContext_InvalidSpanContext(t *testing.T) {
	annotations := make(map[string]string)
	InjectTraceContext(context.Background(), annotations)

	assert.NewCollecting(t).Empty(annotations, "expected no annotations for invalid span, got")
}

func TestExtractTraceContext_WithTracestate(t *testing.T) {
	c := assert.NewAborting(t)
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	Tracer = tp.Tracer(tracerName)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// Create a span and inject context with tracestate.
	ctx, span := Tracer.Start(context.Background(), "webhook")
	ts := trace.TraceState{}
	ts, _ = ts.Insert("vendor", "value")
	ctx = trace.ContextWithSpanContext(ctx, span.SpanContext().WithTraceState(ts))

	annotations := make(map[string]string)
	InjectTraceContext(ctx, annotations)
	span.End()

	// Verify the tracestate was injected under our custom key.
	_, ok := annotations["multigres.com/tracestate"]
	c.True(ok, "expected multigres.com/tracestate annotation")

	// Now extract and verify the tracestate is restored.
	extractedCtx, _ := ExtractTraceContext(annotations)
	sc := trace.SpanFromContext(extractedCtx).SpanContext()
	c.True(sc.IsValid(), "expected valid span context after extraction")
}
