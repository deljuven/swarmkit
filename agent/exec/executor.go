package exec

import (
	"github.com/docker/docker/api/types"
	"github.com/docker/swarmkit/api"
	"golang.org/x/net/context"
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

	// ImageInspect returns image info with the specified image
	ImageInspect(ctx context.Context, image string) (types.ImageInspect, error)

	// ImageList return all images on the underlying node
	ImageList(ctx context.Context) ([]types.ImageSummary, error)

	// GetLayers return all the layers digests on the node
	GetLayers(ctx context.Context, encodedAuth string) ([]string, error)

	// QueryLayersByImage return layer digests of specified image on the underlying node
	QueryLayersByImage(ctx context.Context, image, encodedAuth string) ([]string, error)
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
