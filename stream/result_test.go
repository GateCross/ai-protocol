package stream

import "testing"

func TestObserverSeparatesLogicalChatFinishFromWireTerminal(t *testing.T) {
	var observer Observer
	if err := observer.Feed(Event{Data: `{"choices":[{"delta":{},"finish_reason":"stop"}]}`}); err != nil {
		t.Fatal(err)
	}
	if observer.Result.Terminal {
		t.Fatal("Chat finish_reason was mistaken for the source [DONE] terminal")
	}
	if err := observer.Feed(Event{Data: "[DONE]"}); err != nil {
		t.Fatal(err)
	}
	if !observer.Result.Terminal {
		t.Fatal("source [DONE] was not classified as a wire terminal")
	}
}

func TestObserverUsesSharedFactsForInBandFailure(t *testing.T) {
	var observer Observer
	err := observer.Feed(Event{Data: `{"type":"response.failed","response":{"error":{"message":"overloaded"}}}`})
	if err == nil || err.Error() != "overloaded" {
		t.Fatalf("Observer.Feed error = %v, want overloaded", err)
	}
	if !observer.Result.Terminal || observer.Result.InBandErr == nil {
		t.Fatalf("observer result = %+v, want terminal failure", observer.Result)
	}
}
