package proton

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

// Cannot connect to proton. Notice that this doesn't really necesseary means Proton is down.
// It also can be network issue between Neutron and Proton
var ErrProtonDown = errors.New("failed to connect to Proton")

var (
	codeRe = *regexp.MustCompile(`code: (.+[0-9])`)
	msgRe  = *regexp.MustCompile(`message: (.*)`)
)

func Parse(err error) (int, string) {
	var code int
	var msg string

	errStr := err.Error()
	codeMatches := codeRe.FindStringSubmatch(errStr)
	if len(codeMatches) == 2 {
		code, _ = strconv.Atoi(codeMatches[1])
	}

	msgMatches := msgRe.FindStringSubmatch(errStr)
	if len(msgMatches) == 2 {
		msg = msgMatches[1]
	}

	return code, msg
}

func IsContextCancel(err error) bool {
	code, msg := Parse(err)
	return code == 394 && strings.Contains(msg, "Query was cancelled")
}

// HTTPStatusCode = -1 indicates the error is not from a HTTP request (e.g. from TCP)
type ErrorResponse struct {
	HTTPStatusCode int
	// proton error codes: https://github.com/timeplus-io/proton/blob/develop/src/Common/ErrorCodes.cpp
	Code      int    `json:"code"`
	RequestId string `json:"request_id"`
	Message   string `json:"error_msg"`
}

func (e *ErrorResponse) Error() string {
	if e.Code > 0 {
		return fmt.Sprintf("code: %d, message: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("response code: %d, message: %s", e.HTTPStatusCode, e.Message)
}

func UnmarshalErrorResponse(resp *http.Response) error {
	if resp.StatusCode > 199 && resp.StatusCode < 300 {
		return nil
	}

	e := &ErrorResponse{
		HTTPStatusCode: resp.StatusCode,
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		e.Message = err.Error()
		return e
	}

	if json.Unmarshal(body, e) != nil {
		e.Message = string(body)
	}

	return e
}
