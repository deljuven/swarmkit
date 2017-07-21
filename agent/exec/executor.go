package exec

import (
	"github.com/docker/swarmkit/api"
	"golang.org/x/net/context"
	"github.com/docker/docker/api/types"
)

// Executor provides controllers for tasks.
type Executor interface {
	// Describe returns the underlying node description.
	Describe(ctx context.Context) (*api.NodeDescription, error)

	// Configure uses the node object state to propagate node
	// state to the underlying executor.
	Configure(ctx context.Context, node *api.Node) error

	// Controller provides a controller for the given task.
	Controller(t *api.Task) (Controller, error)

	// SetNetworkBootstrapKeys passes the symmetric keys from the
	// manager to the executor.
	SetNetworkBootstrapKeys([]*api.EncryptionKey) error

	//
	ImageInspect(ctx context.Context, image string) (types.ImageInspect, error)

	//
	GetAllRootFS(ctx context.Context) (map[string]types.RootFS, error)

	ImageList(ctx context.Context) ([]types.ImageSummary, error)
}

// SecretsProvider is implemented by objects that can store secrets, typically
// an executor.
type SecretsProvider interface {
	Secrets() SecretsManager
}

// SecretGetter contains secret data necessary for the Controller.
type SecretGetter interface {
	// Get returns the the secret with a specific secret ID, if available.
	// When the secret is not available, the return will be nil.
	Get(secretID string) *api.Secret
}

// SecretsManager is the interface for secret storage and updates.
type SecretsManager interface {
	SecretGetter

	Add(secrets ...api.Secret) // add one or more secrets
	Remove(secrets []string)   // remove the secrets by ID
	Reset()                    // remove all secrets
}
