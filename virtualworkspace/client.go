package virtualworkspace

import (
	"context"

	"github.com/kcp-dev/logicalcluster/v3"
	"github.com/kcp-dev/multicluster-runtime-provider/internal/kontext"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ client.Client = &workspacedClient{}

// workspacedClient is a client that operates on a specific namespace.
type workspacedClient struct {
	clusterName string
	client.Client
}

// Get returns a single object.
func (n *workspacedClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return n.Client.Get(kontext.WithCluster(ctx, logicalcluster.Name(n.clusterName)), key, obj, opts...)
}

// List returns a list of objects.
func (n *workspacedClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	var copts client.ListOptions
	for _, o := range opts {
		o.ApplyToList(&copts)
	}
	if err := n.Client.List(kontext.WithCluster(ctx, logicalcluster.Name(n.clusterName)), list, opts...); err != nil {
		return err
	}

	return nil
}

// Create creates a new object.
func (n *workspacedClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	return n.Client.Create(kontext.WithCluster(ctx, logicalcluster.Name(n.clusterName)), obj, opts...)
}

// Delete deletes an object.
func (n *workspacedClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	return n.Client.Delete(kontext.WithCluster(ctx, logicalcluster.Name(n.clusterName)), obj, opts...)
}

// Update updates an object.
func (n *workspacedClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	return n.Client.Update(kontext.WithCluster(ctx, logicalcluster.Name(n.clusterName)), obj, opts...)
}

// Patch patches an object.
func (n *workspacedClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	// TODO(sttts): this is not thas easy here. We likely have to support all the different patch types.
	//              But other than that, this is just an example provider, so ¯\_(ツ)_/¯.
	panic("implement the three patch types")
}

// DeleteAllOf deletes all objects of the given type.
func (n *workspacedClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	return n.Client.DeleteAllOf(kontext.WithCluster(ctx, logicalcluster.Name(n.clusterName)), obj, opts...)
}

// Status returns a subresource writer.
func (n *workspacedClient) Status() client.SubResourceWriter {
	return &subResourceWorkspacedClient{clusterName: n.clusterName, client: n.Client.SubResource("status")}
}

// SubResource returns a subresource client.
func (n *workspacedClient) SubResource(subResource string) client.SubResourceClient {
	return &subResourceWorkspacedClient{clusterName: n.clusterName, client: n.Client.SubResource(subResource)}
}

var _ client.SubResourceClient = &subResourceWorkspacedClient{}

// subResourceWorkspacedClient is a client that operates on a specific namespace
// and subresource.
type subResourceWorkspacedClient struct {
	clusterName string
	client      client.SubResourceClient
}

// Get returns a single object from a subresource.
func (s subResourceWorkspacedClient) Get(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceGetOption) error {
	return s.client.Get(kontext.WithCluster(ctx, logicalcluster.Name(s.clusterName)), obj, subResource, opts...)
}

// Create creates a new object in a subresource.
func (s subResourceWorkspacedClient) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	return s.client.Create(kontext.WithCluster(ctx, logicalcluster.Name(s.clusterName)), obj, subResource, opts...)
}

// Update updates an object in a subresource.
func (s subResourceWorkspacedClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	return s.client.Update(kontext.WithCluster(ctx, logicalcluster.Name(s.clusterName)), obj, opts...)
}

// Patch patches an object in a subresource.
func (s subResourceWorkspacedClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	// TODO(sttts): this is not thas easy here. We likely have to support all the different patch types.
	//              But other than that, this is just an example provider, so ¯\_(ツ)_/¯.
	panic("implement the three patch types")
}
