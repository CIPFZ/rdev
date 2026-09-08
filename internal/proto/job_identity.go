package proto

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// PrincipalID length-frames both broker owner fields, so a shared remote
// connection never collapses different client/project principals.
func PrincipalID(client, project string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%s%d:%s", len(client), client, len(project), project)))
	return "principal_" + hex.EncodeToString(sum[:])
}
func JobIDForOperation(principal, operation string) (string, error) {
	if ValidateOperationID(principal) != nil || ValidateOperationID(operation) != nil {
		return "", NewError(CodeInvalidRequest, operation, StateNotSent)
	}
	data, _ := json.Marshal([2]string{principal, operation})
	sum := sha256.Sum256(data)
	return "job_" + hex.EncodeToString(sum[:]), nil
}

// DurableJobDigest excludes transport output windows, which the local Client
// negotiates after the broker records its intent. Job execution parameters are
// otherwise identical to the canonical digest used by the remote cache.
func DurableJobDigest(req *Request) (string, error) {
	if req == nil || req.Op != OpJobStart || req.Job == nil || !req.Job.DurableStart {
		return "", fmt.Errorf("durable job start required")
	}
	copy := *req
	copy.StreamWindowBytes = 0
	return CanonicalRequestDigest(&copy)
}
