package local

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

// localWebAPI is the operator-facing JSON surface used by product-level tests.
// Keeping the operator on HTTP (instead of reaching into workerkit) proves that
// the same actions exposed to the browser are sufficient to complete a call.
type localWebAPI struct {
	base   string
	token  string
	client *http.Client
}

func (api localWebAPI) call(ctx context.Context, method, path string, body any) (map[string]any, int, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, 0, err
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, api.base+path, reader)
	if err != nil {
		return nil, 0, err
	}
	request.Header.Set("Authorization", "Bearer "+api.token)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := api.client.Do(request)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	var decoded map[string]any
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		return nil, response.StatusCode, err
	}
	return decoded, response.StatusCode, nil
}

func webAPIForLocal(t interface{ Fatalf(string, ...any) }, instance *Local, client *http.Client) localWebAPI {
	webURL := instance.WebURL()
	const separator = "/?token="
	index := strings.Index(webURL, separator)
	if index < 0 {
		t.Fatalf("local web URL has no login token: %s", webURL)
	}
	return localWebAPI{base: webURL[:index], token: webURL[index+len(separator):], client: client}
}
