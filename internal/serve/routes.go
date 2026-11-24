package serve

import (
	"net/url"
)

const (
	// AppParam names the app in the path.
	AppParam = "app"
	// CursorParam is the query parameter a consumer resumes with.
	CursorParam = "cursor"
	// changesPattern serves every transport of an app's changes.
	changesPattern = "GET /apps/{" + AppParam + "}/changes"
)

// ChangesPath is the path of an app's changes, resuming after cursor unless
// it is empty.
func ChangesPath(app, cursor string) string {
	path := "/apps/" + url.PathEscape(app) + "/changes"
	if cursor == "" {
		return path
	}
	return path + "?" + url.Values{CursorParam: {cursor}}.Encode()
}
