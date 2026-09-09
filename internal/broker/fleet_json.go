package broker

import "encoding/json"

// Preserve the established decoding of individual routes, while making every
// Fleet RPC as strict as its CLI files and persisted schemas. This runs inside
// the existing bounded/pipelined ingress decoder; duplicate nested fields cannot
// disappear before the server validates a replacement inventory or plan.
func (r *Request) UnmarshalJSON(data []byte) error {
	type wireRequest Request
	var decoded wireRequest
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if isFleetOperation(decoded.Operation) || decoded.Fleet != nil {
		var strict wireRequest
		if err := decodeFleetJSON(data, &strict); err != nil {
			return err
		}
		decoded = strict
	}
	*r = Request(decoded)
	return nil
}
