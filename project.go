package azkaban

import (
	"bytes"
	"errors"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	//"io/ioutil"
	//"crypto/tls"
	//"github.com/davecgh/go-spew/spew"
	//"reflect"
	//"encoding/json"
	"crypto/tls"
	"fmt"
	"io"
)

var ProjectNotFound = errors.New("Project not found")
var ActionType = struct{ Create, Delete string }{"create", "delete"}
var StatusType = struct{ Success, Error string }{"success", "error"}

type Project struct {
	ID   int    `json:"projectId"`
	Name string `json:"project"`
}

type Object struct {
	Project string `json:"project"`
	Action  string `json:"-"`
	Status  string `json:"status"`
}

type Upload struct {
	Error     string `json:"error"`
	Version   string `json:"version"`
	IdProject int    `json:"projectId"`
}

// The ajax API for getting an existing project.
func (this *Client) GetProject(name string) (*Project, error) {
	flows, err := this.FetchFlows(name)
	return &flows.Project, err
}

// The ajax API for creating a new project.
func (this *Client) CreateProject(name, description string) (*Object, error) {

	// init return
	action := Object{
		Project: name,
		Action:  ActionType.Create,
	}

	// set form parameters
	values := url.Values{}
	values.Add("action", "create")
	values.Add("name", name)
	values.Add("description", description)
	values.Add("session.id", this.Session)

	// try to create a project
	err := this.action(http.MethodPost, "/manager", values, &action)

	return &action, err

}

// The ajax API for deleting an existing project.
func (this *Client) DeleteProject(name string) (*Object, error) {

	// init return
	object := Object{
		Project: name,
		Action:  ActionType.Delete,
		Status:  StatusType.Error,
	}

	// check if project exists
	_, err := this.GetProject(name)

	// project exists
	if err == nil {

		// set query string
		values := url.Values{}
		values.Add("delete", "true")
		values.Add("project", name)
		values.Add("session.id", this.Session)

		// this endpoint does not have a return
		var html string

		// try to delete the project
		err = this.action(http.MethodGet, "/manager", values, &html)

		// was it deleted?
		if _, err = this.GetProject(name); err == ProjectNotFound {

			// yes!
			err = nil
			object.Status = StatusType.Success

		}

	}

	return &object, err

}

func (this *Client) UploadProjectZip(project, file string) error {

	// Prepare a form that you will submit to azkaban
	var buff bytes.Buffer
	w := multipart.NewWriter(&buff)

	// Add project file
	f, err := os.Open(file)
	defer f.Close()
	if err != nil {
		return err
	}
	fw, err := w.CreateFormFile("file", filepath.Base(file))
	if err != nil {
		return err
	}
	if _, err = io.Copy(fw, f); err != nil {
		return err
	}
	// Add the other fields
	if fw, err = w.CreateFormField("ajax"); err != nil {
		return err
	}
	if _, err = fw.Write([]byte("upload")); err != nil {
		return err
	}
	if fw, err = w.CreateFormField("project"); err != nil {
		return err
	}
	if _, err = fw.Write([]byte(project)); err != nil {
		return err
	}
	if fw, err = w.CreateFormField("session.id"); err != nil {
		return err
	}
	if _, err = fw.Write([]byte(this.Session)); err != nil {
		return err
	}
	// Don't forget to close the multipart writer.
	// If you don't close it, your request will be missing the terminating boundary.
	w.Close()

	// Now that you have a form, you can submit it to your handler.
	req, err := http.NewRequest(http.MethodPost, this.Endpoint+"/manager", &buff)
	if err != nil {
		return err
	}

	// Don't forget to set the content type, this will contain the boundary.
	req.Header.Set("Content-Type", w.FormDataContentType())

	// init http.Client without ssl verification
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}

	// Submit the request
	res, err := client.Do(req)
	if err != nil {
		return err
	}

	// Check the response
	if res.StatusCode != http.StatusOK {
		err = fmt.Errorf("bad status: %s", res.Status)
	}

	return err

}
