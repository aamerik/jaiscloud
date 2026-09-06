package sdkv1_test

import (
	"context"
	"testing"

	"google.golang.org/api/cloudfunctions/v1"
	"google.golang.org/api/option"

	"github.com/stretchr/testify/require"
)

// TestSDKCloudFunctions exercises the Cloud Functions v1 REST apiary client
// against the emulator: create (by resource name in the body), get, list, call,
// and delete.
func TestSDKCloudFunctions(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudfunctions.NewService(ctx, option.WithEndpoint(endpoint()), option.WithoutAuthentication())
	require.NoError(t, err)

	const parent = "projects/proj/locations/us-central1"
	name := parent + "/functions/" + unique("fn")

	// Create returns an Operation in real GCP; the emulator completes
	// synchronously and returns the function resource, so we only assert the
	// call succeeds and then verify via Get.
	_, err = svc.Projects.Locations.Functions.Create(parent, &cloudfunctions.CloudFunction{
		Name:       name,
		Runtime:    "nodejs20",
		EntryPoint: "helloWorld",
	}).Do()
	require.NoError(t, err)

	fn, err := svc.Projects.Locations.Functions.Get(name).Do()
	require.NoError(t, err)
	require.Equal(t, name, fn.Name)
	require.Equal(t, "nodejs20", fn.Runtime)
	require.Equal(t, "helloWorld", fn.EntryPoint)
	require.Equal(t, "ACTIVE", fn.Status)

	list, err := svc.Projects.Locations.Functions.List(parent).Do()
	require.NoError(t, err)
	require.NotEmpty(t, list.Functions)

	call, err := svc.Projects.Locations.Functions.Call(name, &cloudfunctions.CallFunctionRequest{
		Data: "ping",
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "ping", call.Result)
	require.NotEmpty(t, call.ExecutionId)

	_, err = svc.Projects.Locations.Functions.Delete(name).Do()
	require.NoError(t, err)

	_, err = svc.Projects.Locations.Functions.Get(name).Do()
	require.Error(t, err)
}
