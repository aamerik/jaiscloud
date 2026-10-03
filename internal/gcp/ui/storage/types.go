package storageui

// Bucket is the UI representation of a Cloud Storage bucket.
type Bucket struct {
	Name         string `json:"name"`
	Location     string `json:"location,omitempty"`
	StorageClass string `json:"storageClass,omitempty"`
	TimeCreated  string `json:"timeCreated,omitempty"`
	Updated      string `json:"updated,omitempty"`
	Versioning   bool   `json:"versioning"`
}

// ListBucketsResponse is the response for GET /buckets.
type ListBucketsResponse struct {
	Items []Bucket `json:"items"`
	Total int      `json:"total"`
}

// GCSObject is the UI representation of a Cloud Storage object.
type GCSObject struct {
	Name         string `json:"name"`
	Size         string `json:"size,omitempty"`
	ContentType  string `json:"contentType,omitempty"`
	StorageClass string `json:"storageClass,omitempty"`
	Updated      string `json:"updated,omitempty"`
	MD5          string `json:"md5Hash,omitempty"`
}

// ListObjectsResponse is the response for GET /buckets/{bucket}/objects.
type ListObjectsResponse struct {
	Items         []GCSObject `json:"items"`
	Prefixes      []string    `json:"prefixes,omitempty"`
	NextPageToken string      `json:"nextPageToken,omitempty"`
}

// CreateBucketRequest is the body for POST /buckets.
type CreateBucketRequest struct {
	Name         string `json:"name"`
	Location     string `json:"location,omitempty"`
	StorageClass string `json:"storageClass,omitempty"`
}
