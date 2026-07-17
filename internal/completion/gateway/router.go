package gateway

import (
	"context"
	"errors"
	"net/http"

	"github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/completion"
)

// ErrWorkerRouteDenied is a policy decision, not a routing-system failure and
// not worker unavailability. The HTTP surface turns it into a finite 403
// before reserving queue capacity or creating durable task state.
var ErrWorkerRouteDenied = errors.New("worker route denied")

// WorkerRouteRequest contains the authenticated and decoded correctness
// identity used to choose the initial owner of a task. HTTPRequest is a clone
// with a fresh, readable body and the original request context.
type WorkerRouteRequest struct {
	HTTPRequest *http.Request
	Caller      auth.Principal
	Model       string
	Tier        completion.CapabilityTier
	Identity    completion.RoutingIdentity
}

// WorkerRouter selects the exact authenticated worker subject for a new task.
// An empty result explicitly requests the hub's deterministic default route.
type WorkerRouter interface {
	RouteWorker(context.Context, WorkerRouteRequest) (string, error)
}
