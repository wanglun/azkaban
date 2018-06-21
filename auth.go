package azkaban

import (
	"net/http"
	"net/url"
)

// This API authenticates a user and provides a session.id in response.
func (this *Client) Authenticate(username, password string) error {

	// set form parameters
	values := url.Values{}
	values.Add("action", "login")
	values.Add("username", username)
	values.Add("password", password)

	// init session
	return this.action(http.MethodPost, "/", values, this)

}
