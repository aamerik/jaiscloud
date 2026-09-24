package functions

import (
	"context"
	"errors"
	"time"

	lambdaexec "jaiscloud/internal/executor/lambda"
	"jaiscloud/internal/gcp/paging"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/model"
)

// CreateFunction creates a function and returns it with its done create
// operation. project/location scope the store; id is the explicit functionId
// parameter (which wins over any name in the input). The runtime is required.
func (s *Service) CreateFunction(ctx context.Context, project, location, id string, in FunctionInput, v Version) (functionsstore.Function, Operation, error) {
	if location == "" {
		return functionsstore.Function{}, Operation{}, invalidArgument("missing location")
	}
	id, err := resolveCreateID(in.Name, location, id)
	if err != nil {
		return functionsstore.Function{}, Operation{}, err
	}
	if id == "" {
		return functionsstore.Function{}, Operation{}, invalidArgument("missing functionId")
	}
	if in.Runtime == "" {
		return functionsstore.Function{}, Operation{}, invalidArgument("missing runtime")
	}
	f := newFunction(project, location, id, in)
	if err := s.functions.CreateFunction(ctx, project, location, id, f); err != nil {
		if errors.Is(err, functionsstore.ErrAlreadyExists) {
			return functionsstore.Function{}, Operation{}, model.NewProviderError("AlreadyExists", "function already exists", 409)
		}
		return functionsstore.Function{}, Operation{}, err
	}
	target := resourceID(project)("cloud-function", location+"/"+id)
	return f, NewOperation(location, "create", target, &f), nil
}

// GetFunction returns one function.
func (s *Service) GetFunction(ctx context.Context, project, location, id string) (functionsstore.Function, error) {
	if location == "" || id == "" {
		return functionsstore.Function{}, invalidArgument("missing location or function name")
	}
	f, err := s.functions.GetFunction(ctx, project, location, id)
	if err != nil {
		return functionsstore.Function{}, mapErr(err)
	}
	return f, nil
}

// requireFunction returns NotFound when the function does not exist.
func (s *Service) requireFunction(ctx context.Context, project, location, id string) error {
	_, err := s.GetFunction(ctx, project, location, id)
	return err
}

// ListFunctions returns a cursor page of functions. The location "-" is GCP's
// all-locations wildcard, aggregating across every region for the project.
func (s *Service) ListFunctions(ctx context.Context, project, location string, pageSize int, pageToken string) ([]functionsstore.Function, string, error) {
	var (
		fns []functionsstore.Function
		err error
	)
	if location == "-" {
		fns, err = s.functions.ListFunctionsAllLocations(ctx, project)
	} else {
		fns, err = s.functions.ListFunctions(ctx, project, location)
	}
	if err != nil {
		return nil, "", err
	}
	page, next := paging.Page(fns, func(f functionsstore.Function) string { return f.ID }, pageParams(pageSize, pageToken))
	return page, next, nil
}

// UpdateFunction merges the input into the stored function under the given mask
// and returns it with its done update operation.
func (s *Service) UpdateFunction(ctx context.Context, project, location, id string, in FunctionInput, mask []string, v Version) (functionsstore.Function, Operation, error) {
	if location == "" || id == "" {
		return functionsstore.Function{}, Operation{}, invalidArgument("missing location or function name")
	}
	f, err := s.functions.UpdateFunctionAtomic(ctx, project, location, id, func(f functionsstore.Function) (functionsstore.Function, error) {
		if uerr := ApplyFunctionUpdate(&f, in, mask); uerr != nil {
			return functionsstore.Function{}, uerr
		}
		return f, nil
	})
	if err != nil {
		return functionsstore.Function{}, Operation{}, mapErr(err)
	}
	target := resourceID(project)("cloud-function", location+"/"+id)
	return f, NewOperation(location, "update", target, &f), nil
}

// DeleteFunction deletes a function and returns its done delete operation (whose
// response is empty).
func (s *Service) DeleteFunction(ctx context.Context, project, location, id string) (Operation, error) {
	if location == "" || id == "" {
		return Operation{}, invalidArgument("missing location or function name")
	}
	if err := s.functions.DeleteFunction(ctx, project, location, id); err != nil {
		return Operation{}, mapErr(err)
	}
	target := resourceID(project)("cloud-function", location+"/"+id)
	return NewOperation(location, "delete", target, nil), nil
}

// CallFunction invokes a function synchronously via the Lambda executor. The
// executor (mock echo by default, Docker/K8s under JAISCLOUD_EXECUTOR_MODE)
// runs the function's entryPoint and returns the result as a string. An executor
// failure is reported as invokeErr with a nil error so the wire response carries
// it (matching real Cloud Functions, which returns the function error in-band).
func (s *Service) CallFunction(ctx context.Context, project, location, id, data string) (executionID, result, invokeErr string, err error) {
	f, err := s.GetFunction(ctx, project, location, id)
	if err != nil {
		return "", "", "", err
	}
	timeout, terr := time.ParseDuration(f.Timeout)
	if terr != nil || timeout <= 0 {
		timeout = defaultFunctionTimeout
	}
	invCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req := lambdaexec.InvokeRequest{
		FunctionName: f.ID,
		Runtime:      f.Runtime,
		Handler:      f.EntryPoint,
		EnvVars:      f.EnvironmentVariables,
		Payload:      []byte(data),
		AccountID:    project,
		MemoryMB:     f.AvailableMemoryMB,
		TimeoutSecs:  int(timeout.Seconds()),
	}
	executionID = newUUID()
	res, ierr := s.executor.Invoke(invCtx, req)
	if ierr != nil {
		return executionID, "", ierr.Error(), nil
	}
	return executionID, string(res.Payload), "", nil
}

// GenerateUploadURL returns a fake signed upload URL for source deployment.
func (s *Service) GenerateUploadURL(project, location string) string {
	return "https://storage.googleapis.com/uploads/" + project + "/" + location + "/" + newUUID() + ".zip"
}

// GenerateDownloadURL returns a fake signed download URL for a function's source
// archive. The function must exist (NotFound otherwise), matching real Cloud
// Functions.
func (s *Service) GenerateDownloadURL(ctx context.Context, project, location, id string) (string, error) {
	if err := s.requireFunction(ctx, project, location, id); err != nil {
		return "", err
	}
	return "https://storage.googleapis.com/" + project + "-cloudfunctions/" + location + "/" + id + ".zip", nil
}
