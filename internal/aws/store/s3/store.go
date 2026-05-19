// Package s3 provides AWS S3 metadata store implementations.
// The canonical interface and types are defined in internal/store/object.
package s3

import objectstore "jaiscloud/internal/aws/store/object"

// Type aliases so memory.go and postgres.go use the canonical types without change.
type ObjectMeta = objectstore.ObjectMeta
type PartMeta = objectstore.PartMeta
type ActiveUpload = objectstore.ActiveUpload
