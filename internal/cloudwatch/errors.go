package cloudwatch

import (
	"encoding/xml"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
)

// xmlNamespace is the CloudWatch query-protocol document namespace. The SDKs
// do not validate it, but real tooling logs a mismatch.
const xmlNamespace = "http://monitoring.amazonaws.com/doc/2010-08-01/"

// AWS error fault types. Sender means the caller must change the request;
// Receiver means the service failed and a retry may work.
const (
	faultSender   = "Sender"
	faultReceiver = "Receiver"
)

// apiError is a CloudWatch error rendered as an ErrorResponse document.
type apiError struct {
	Code    string
	Message string
	Status  int
	Fault   string
}

func (e *apiError) Error() string { return e.Code + ": " + e.Message }

func senderError(code, message string, status int) *apiError {
	return &apiError{Code: code, Message: message, Status: status, Fault: faultSender}
}

func invalidParameter(name, reason string) *apiError {
	return senderError("InvalidParameterValue",
		fmt.Sprintf("The parameter %s is not valid: %s", name, reason), http.StatusBadRequest)
}

func missingParameter(name string) *apiError {
	return senderError("MissingParameter",
		fmt.Sprintf("The parameter %s is required.", name), http.StatusBadRequest)
}

func invalidAction(action string) *apiError {
	return senderError("InvalidAction",
		fmt.Sprintf("The action %s is not valid for this web service.", action), http.StatusBadRequest)
}

func accessDenied(message string) *apiError {
	return senderError("AccessDenied", message, http.StatusForbidden)
}

// internalError hides the underlying cause from the caller and leaves it in
// the log, so a storage fault does not leak paths or label values.
func internalError(log *slog.Logger, action string, err error) *apiError {
	log.Error("cloudwatch request failed", "action", action, "error", err)
	return &apiError{
		Code:    "InternalFailure",
		Message: "The request processing has failed because of an unknown error.",
		Status:  http.StatusInternalServerError,
		Fault:   faultReceiver,
	}
}

// WriteAuthError renders an authentication failure in the CloudWatch error
// shape. It is exported for the auth middleware, which rejects a request
// before any handler in this package sees it.
func WriteAuthError(w http.ResponseWriter, log *slog.Logger, code, message string, status int) {
	writeError(w, log, uuid.NewString(), senderError(code, message, status))
}

// errorResponse is the wire form of a rejected request.
type errorResponse struct {
	XMLName   xml.Name `xml:"ErrorResponse"`
	Namespace string   `xml:"xmlns,attr"`
	Error     struct {
		Type    string `xml:"Type"`
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	} `xml:"Error"`
	RequestID string `xml:"RequestId"`
}

// writeError renders err as an ErrorResponse. A non-apiError is reported as an
// internal failure rather than surfacing its text, which may name internals.
func writeError(w http.ResponseWriter, log *slog.Logger, requestID string, err error) {
	var apiErr *apiError
	if !errors.As(err, &apiErr) {
		apiErr = internalError(log, "", err)
	}

	var resp errorResponse
	resp.Namespace = xmlNamespace
	resp.Error.Type = apiErr.Fault
	resp.Error.Code = apiErr.Code
	resp.Error.Message = apiErr.Message
	resp.RequestID = requestID

	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(apiErr.Status)
	if err := writeXML(w, resp); err != nil {
		log.Warn("writing cloudwatch error response", "error", err)
	}
}

// writeXML emits a document with the XML declaration AWS clients expect.
func writeXML(w http.ResponseWriter, doc any) error {
	if _, err := w.Write([]byte(xml.Header)); err != nil {
		return err
	}
	enc := xml.NewEncoder(w)
	if err := enc.Encode(doc); err != nil {
		return err
	}
	return enc.Flush()
}
