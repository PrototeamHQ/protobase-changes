package serve

import (
	"net/url"
)

const (
	// AppParam names the app in the path.
	AppParam = "app"
	// CursorParam is the query parameter a consumer resumes with.
	CursorParam = "cursor"
	// DestinationParam names a destination in the path.
	DestinationParam = "destination"
	// changesPattern serves every transport of an app's changes.
	changesPattern = "GET /apps/{" + AppParam + "}/changes"
	// destinationsPattern takes the token that defines an app's destinations.
	destinationsPattern = "PUT /apps/{" + AppParam + "}/destinations"
	// resumePattern resumes a paused or gone destination.
	resumePattern = "POST /apps/{" + AppParam + "}/destinations/{" + DestinationParam + "}/resume"
	// relayPattern serves the relay's connection, for every app its key covers.
	relayPattern = "GET " + RelayPath
)

// RelayPath is the path of the relay's connection.
const RelayPath = "/relay"

// DestinationsPath is the path an app puts its destinations to.
func DestinationsPath(app string) string {
	return "/apps/" + url.PathEscape(app) + "/destinations"
}

// ResumePath is the path that resumes one of an app's destinations.
func ResumePath(app, destination string) string {
	return DestinationsPath(app) + "/" + url.PathEscape(destination) + "/resume"
}

// ChangesPath is the path of an app's changes, resuming after cursor unless
// it is empty.
func ChangesPath(app, cursor string) string {
	path := "/apps/" + url.PathEscape(app) + "/changes"
	if cursor == "" {
		return path
	}
	return path + "?" + url.Values{CursorParam: {cursor}}.Encode()
}
