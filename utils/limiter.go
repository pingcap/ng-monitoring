package utils

// RateLimit wraps a fix sized channel to control concurrency.
type RateLimit struct {
	capacity int
	token    chan struct{}
}

// NewRateLimit creates a limit controller with capacity n.
func NewRateLimit(n int) *RateLimit {
	return &RateLimit{
		capacity: n,
		token:    make(chan struct{}, n),
	}
}

// GetToken acquires a token.
func (r *RateLimit) GetToken(done <-chan struct{}) (exit bool) {
	select {
	case <-done:
		return true
	case r.token <- struct{}{}:
		return false
	}
}

// PutToken puts a token back.
func (r *RateLimit) PutToken() {
	select {
	case <-r.token:
	default:
		panic("put a redundant token")
	}
}

// GetCapacity returns the token capacity.
func (r *RateLimit) GetCapacity() int {
	return r.capacity
}
