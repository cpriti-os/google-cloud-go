// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"

	"cloud.google.com/go/iam/apiv1/iampb"
)

type metricsWrappingClient struct {
	client      storageClient
	instruments *metricInstruments
	isGRPC      bool
}

func (m *metricsWrappingClient) wrap(ctx context.Context) context.Context {
	if m.instruments != nil {
		ctx = context.WithValue(ctx, metricInstrumentsKey, m.instruments)
		ctx = context.WithValue(ctx, transportKey, m.isGRPC)
	}
	return ctx
}

func (m *metricsWrappingClient) getSettings() *settings {
	return m.client.getSettings()
}

func (m *metricsWrappingClient) Close() error {
	return m.client.Close()
}

func (m *metricsWrappingClient) GetServiceAccount(ctx context.Context, project string, opts ...storageOption) (string, error) {
	return m.client.GetServiceAccount(m.wrap(ctx), project, opts...)
}

func (m *metricsWrappingClient) CreateBucket(ctx context.Context, project, bucket string, attrs *BucketAttrs, enableObjectRetention *bool, opts ...storageOption) (*BucketAttrs, error) {
	return m.client.CreateBucket(m.wrap(ctx), project, bucket, attrs, enableObjectRetention, opts...)
}

func (m *metricsWrappingClient) ListBuckets(ctx context.Context, project string, opts ...storageOption) *BucketIterator {
	return m.client.ListBuckets(m.wrap(ctx), project, opts...)
}

func (m *metricsWrappingClient) DeleteBucket(ctx context.Context, bucket string, conds *BucketConditions, opts ...storageOption) error {
	return m.client.DeleteBucket(m.wrap(ctx), bucket, conds, opts...)
}

func (m *metricsWrappingClient) GetBucket(ctx context.Context, bucket string, conds *BucketConditions, opts ...storageOption) (*BucketAttrs, error) {
	return m.client.GetBucket(m.wrap(ctx), bucket, conds, opts...)
}

func (m *metricsWrappingClient) UpdateBucket(ctx context.Context, bucket string, uattrs *BucketAttrsToUpdate, conds *BucketConditions, opts ...storageOption) (*BucketAttrs, error) {
	return m.client.UpdateBucket(m.wrap(ctx), bucket, uattrs, conds, opts...)
}

func (m *metricsWrappingClient) LockBucketRetentionPolicy(ctx context.Context, bucket string, conds *BucketConditions, opts ...storageOption) error {
	return m.client.LockBucketRetentionPolicy(m.wrap(ctx), bucket, conds, opts...)
}

func (m *metricsWrappingClient) ListObjects(ctx context.Context, bucket string, q *Query, opts ...storageOption) *ObjectIterator {
	return m.client.ListObjects(m.wrap(ctx), bucket, q, opts...)
}

func (m *metricsWrappingClient) DeleteObject(ctx context.Context, bucket, object string, gen int64, conds *Conditions, opts ...storageOption) error {
	return m.client.DeleteObject(m.wrap(ctx), bucket, object, gen, conds, opts...)
}

func (m *metricsWrappingClient) GetObject(ctx context.Context, params *getObjectParams, opts ...storageOption) (*ObjectAttrs, error) {
	return m.client.GetObject(m.wrap(ctx), params, opts...)
}

func (m *metricsWrappingClient) UpdateObject(ctx context.Context, params *updateObjectParams, opts ...storageOption) (*ObjectAttrs, error) {
	return m.client.UpdateObject(m.wrap(ctx), params, opts...)
}

func (m *metricsWrappingClient) RestoreObject(ctx context.Context, params *restoreObjectParams, opts ...storageOption) (*ObjectAttrs, error) {
	return m.client.RestoreObject(m.wrap(ctx), params, opts...)
}

func (m *metricsWrappingClient) MoveObject(ctx context.Context, params *moveObjectParams, opts ...storageOption) (*ObjectAttrs, error) {
	return m.client.MoveObject(m.wrap(ctx), params, opts...)
}

func (m *metricsWrappingClient) DeleteDefaultObjectACL(ctx context.Context, bucket string, entity ACLEntity, opts ...storageOption) error {
	return m.client.DeleteDefaultObjectACL(m.wrap(ctx), bucket, entity, opts...)
}

func (m *metricsWrappingClient) ListDefaultObjectACLs(ctx context.Context, bucket string, opts ...storageOption) ([]ACLRule, error) {
	return m.client.ListDefaultObjectACLs(m.wrap(ctx), bucket, opts...)
}

func (m *metricsWrappingClient) UpdateDefaultObjectACL(ctx context.Context, bucket string, entity ACLEntity, role ACLRole, opts ...storageOption) error {
	return m.client.UpdateDefaultObjectACL(m.wrap(ctx), bucket, entity, role, opts...)
}

func (m *metricsWrappingClient) DeleteBucketACL(ctx context.Context, bucket string, entity ACLEntity, opts ...storageOption) error {
	return m.client.DeleteBucketACL(m.wrap(ctx), bucket, entity, opts...)
}

func (m *metricsWrappingClient) ListBucketACLs(ctx context.Context, bucket string, opts ...storageOption) ([]ACLRule, error) {
	return m.client.ListBucketACLs(m.wrap(ctx), bucket, opts...)
}

func (m *metricsWrappingClient) UpdateBucketACL(ctx context.Context, bucket string, entity ACLEntity, role ACLRole, opts ...storageOption) error {
	return m.client.UpdateBucketACL(m.wrap(ctx), bucket, entity, role, opts...)
}

func (m *metricsWrappingClient) DeleteObjectACL(ctx context.Context, bucket, object string, entity ACLEntity, opts ...storageOption) error {
	return m.client.DeleteObjectACL(m.wrap(ctx), bucket, object, entity, opts...)
}

func (m *metricsWrappingClient) ListObjectACLs(ctx context.Context, bucket, object string, opts ...storageOption) ([]ACLRule, error) {
	return m.client.ListObjectACLs(m.wrap(ctx), bucket, object, opts...)
}

func (m *metricsWrappingClient) UpdateObjectACL(ctx context.Context, bucket, object string, entity ACLEntity, role ACLRole, opts ...storageOption) error {
	return m.client.UpdateObjectACL(m.wrap(ctx), bucket, object, entity, role, opts...)
}

func (m *metricsWrappingClient) ComposeObject(ctx context.Context, req *composeObjectRequest, opts ...storageOption) (*ObjectAttrs, error) {
	return m.client.ComposeObject(m.wrap(ctx), req, opts...)
}

func (m *metricsWrappingClient) RewriteObject(ctx context.Context, req *rewriteObjectRequest, opts ...storageOption) (*rewriteObjectResponse, error) {
	return m.client.RewriteObject(m.wrap(ctx), req, opts...)
}

func (m *metricsWrappingClient) NewRangeReader(ctx context.Context, params *newRangeReaderParams, opts ...storageOption) (*Reader, error) {
	return m.client.NewRangeReader(m.wrap(ctx), params, opts...)
}

func (m *metricsWrappingClient) OpenWriter(params *openWriterParams, opts ...storageOption) (internalWriter, error) {
	params.ctx = m.wrap(params.ctx)
	return m.client.OpenWriter(params, opts...)
}

func (m *metricsWrappingClient) GetIamPolicy(ctx context.Context, resource string, version int32, opts ...storageOption) (*iampb.Policy, error) {
	return m.client.GetIamPolicy(m.wrap(ctx), resource, version, opts...)
}

func (m *metricsWrappingClient) SetIamPolicy(ctx context.Context, resource string, policy *iampb.Policy, opts ...storageOption) error {
	return m.client.SetIamPolicy(m.wrap(ctx), resource, policy, opts...)
}

func (m *metricsWrappingClient) TestIamPermissions(ctx context.Context, resource string, permissions []string, opts ...storageOption) ([]string, error) {
	return m.client.TestIamPermissions(m.wrap(ctx), resource, permissions, opts...)
}

func (m *metricsWrappingClient) GetHMACKey(ctx context.Context, project, accessID string, opts ...storageOption) (*HMACKey, error) {
	return m.client.GetHMACKey(m.wrap(ctx), project, accessID, opts...)
}

func (m *metricsWrappingClient) ListHMACKeys(ctx context.Context, project, serviceAccountEmail string, showDeletedKeys bool, opts ...storageOption) *HMACKeysIterator {
	return m.client.ListHMACKeys(m.wrap(ctx), project, serviceAccountEmail, showDeletedKeys, opts...)
}

func (m *metricsWrappingClient) UpdateHMACKey(ctx context.Context, project, serviceAccountEmail, accessID string, attrs *HMACKeyAttrsToUpdate, opts ...storageOption) (*HMACKey, error) {
	return m.client.UpdateHMACKey(m.wrap(ctx), project, serviceAccountEmail, accessID, attrs, opts...)
}

func (m *metricsWrappingClient) CreateHMACKey(ctx context.Context, project, serviceAccountEmail string, opts ...storageOption) (*HMACKey, error) {
	return m.client.CreateHMACKey(m.wrap(ctx), project, serviceAccountEmail, opts...)
}

func (m *metricsWrappingClient) DeleteHMACKey(ctx context.Context, project, accessID string, opts ...storageOption) error {
	return m.client.DeleteHMACKey(m.wrap(ctx), project, accessID, opts...)
}

func (m *metricsWrappingClient) ListNotifications(ctx context.Context, bucket string, opts ...storageOption) (map[string]*Notification, error) {
	return m.client.ListNotifications(m.wrap(ctx), bucket, opts...)
}

func (m *metricsWrappingClient) CreateNotification(ctx context.Context, bucket string, n *Notification, opts ...storageOption) (*Notification, error) {
	return m.client.CreateNotification(m.wrap(ctx), bucket, n, opts...)
}

func (m *metricsWrappingClient) DeleteNotification(ctx context.Context, bucket string, id string, opts ...storageOption) error {
	return m.client.DeleteNotification(m.wrap(ctx), bucket, id, opts...)
}

func (m *metricsWrappingClient) NewMultiRangeDownloader(ctx context.Context, params *newMultiRangeDownloaderParams, opts ...storageOption) (*MultiRangeDownloader, error) {
	return m.client.NewMultiRangeDownloader(m.wrap(ctx), params, opts...)
}

func wrapWithMetrics(tc storageClient) storageClient {
	if tc != nil && tc.getSettings() != nil && tc.getSettings().metricsContext != nil && tc.getSettings().metricsContext.instruments != nil {
		_, isGRPC := tc.(*grpcStorageClient)
		return &metricsWrappingClient{client: tc, instruments: tc.getSettings().metricsContext.instruments, isGRPC: isGRPC}
	}
	return tc
}
