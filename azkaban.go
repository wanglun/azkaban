package azkaban

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"github.com/davecgh/go-spew/spew"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strings"
)

var EmptyResponse = errors.New("Empty response")

// Client is the base struct for requests
type Client struct {
	Endpoint string
	Session  string `json:"session.id"`
	Status   string `json:"status"`
}

type Detail struct {
	Error   string `json:"error"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

// New is used to create a new Client based off of the Endpoint
func New(endpoint string) *Client {

	return &Client{
		Endpoint: endpoint,
	}

}

func (this *Client) action(method, route string, values url.Values, data interface{}) error {

	// init vars
	var (
		err      error
		request  *http.Request
		response *http.Response
		content  []byte
	)

	// create request
	if request, err = http.NewRequest(method, this.Endpoint+route, strings.NewReader(values.Encode())); err == nil {

		defer request.Body.Close()

		// set header parameters required by azkaban
		if method == http.MethodPost {
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			request.Header.Set("X-Requested-With", "XMLHttpRequest")
		}

		// parse query string
		if method == http.MethodGet {
			request.URL.RawQuery = values.Encode()
		}

		// init http.Client without ssl verification
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}

		// do request
		if response, err = client.Do(request); err == nil {

			defer response.Body.Close()

			// parse response into Session
			if content, err = ioutil.ReadAll(response.Body); err == nil {

				// incorrect login?
				if err = json.Unmarshal(content, &data); err == nil {

					// check if request had failed
					var detail Detail

					// parse response into Fail
					if err = json.Unmarshal(content, &detail); err == nil {

						// return azkaban error as a go error
						if detail.Error != "" {
							err = errors.New(detail.Error)
						} else if detail.Status == "error" {
							err = errors.New(detail.Message)
						}

					}

				} else {

					// check for html response
					switch err.(type) {

					case *json.SyntaxError:

						if len(content) == 0 {
							return EmptyResponse
						}

						spew.Dump(string(content))

						value := reflect.ValueOf(data)
						value.Elem().SetString(string(content))

					}

				}

			}

		}

	}

	return err

}
