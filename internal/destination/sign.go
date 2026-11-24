package destination

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	standardwebhooks "github.com/standard-webhooks/standard-webhooks/libraries/go"
)

// newSigner reads a Standard Webhooks secret, whsec_ and base64.
func newSigner(secret string) (*standardwebhooks.Webhook, error) {
	if !strings.HasPrefix(secret, "whsec_") {
		return nil, errors.New("not a whsec_ secret")
	}
	return standardwebhooks.NewWebhook(secret)
}

// sign sets the Standard Webhooks headers of one request: its id, the same
// on every try, its time, and the signature of both with the body.
func sign(h http.Header, secret, id string, at time.Time, body []byte) error {
	signer, err := newSigner(secret)
	if err != nil {
		return err
	}
	signature, err := signer.Sign(id, at, body)
	if err != nil {
		return err
	}
	h.Set(standardwebhooks.HeaderWebhookID, id)
	h.Set(standardwebhooks.HeaderWebhookTimestamp, strconv.FormatInt(at.Unix(), 10))
	h.Set(standardwebhooks.HeaderWebhookSignature, signature)
	return nil
}
