package azkaban

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/metakeule/fmtdate"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Flow struct {
	IdFlow string `json:"flowId"`
}

type Flows struct {
	Project
	Flows []Flow `json:"flows"`
}

type Node struct {
	ID   string   `json:"id"`
	Type string   `json:"type"`
	In   []string `json:"in"`
}

type Jobs struct {
	Project
	Flow  string `json:"flow"`
	Nodes []Node `json:"nodes"`
}

type Execution struct {
	IdExecution int       `json:"execId"`
	IdProject   int       `json:"projectId"`
	IdFlow      string    `json:"flowId"`
	User        string    `json:"submitUser"`
	SubmitTime  int64     `json:"submitTime"`
	StartTime   int64     `json:"startTime"`
	EndTime     int64     `json:"endTime"`
	SubmitAt    time.Time `json:"submitAt"`
	StartedAd   time.Time `json:"startedAd"`
	FinishedAt  time.Time `json:"finishedAt"`
	Status      string    `json:"status"`
}

type Executions struct {
	Execution []Execution `json:"executions"`
	Project   string      `json:"project"`
	IdProject int         `json:"projectId"`
	IdFlow    string      `json:"flow"`
	Start     int         `json:"from"`
	Length    int         `json:"length"`
	Total     int         `json:"total"`
}

type Running struct {
	IdsExecution []string `json:"execIds"`
}

type Execute struct {
	IdExecution int64  `json:"execid"`
	Message     string `json:"message"`
	Project     string `json:"project"`
	Flow        string `json:"flow"`
}

// used to avoid recursion in UnmarshalJSON below
type execution Execution

// override json.Unmarshal for Execution
func (e *Execution) UnmarshalJSON(b []byte) (err error) {

	x := execution{}

	if err = json.Unmarshal(b, &x); err == nil {
		*e = Execution(x)
		e.SubmitAt = time.Unix(e.SubmitTime/1000, 0)
		e.StartedAd = time.Unix(e.StartTime/1000, 0)
		e.FinishedAt = time.Unix(e.EndTime/1000, 0)
		return
	}

	return nil
}

// create a new Decode for Execution
func Decode(r io.Reader) (exe *Execution, err error) {
	exe = new(Execution)
	return
}

func (this *Client) CreateCommandJob(project, job string, commands ...string) error {

	// get default temp dir
	temp := "/tmp/"

	// windows?
	if strings.Contains(os.Getenv("OS"), "Windows") {
		temp = "C:\\Windows\\Temp\\"
	}

	// mount file path
	jobname := fmt.Sprintf("%s%s.job", temp, job)
	zipname := fmt.Sprintf("%s%s.zip", temp, job)

	// write job file
	if err := WriteFile(jobname); err != nil {
		return err
	}

	// zip job file
	if err := ZipFiles(zipname, jobname); err != nil {
		return err
	}

	// upload zip file to project
	if err := this.UploadProjectZip(project, job, zipname); err != nil {
		return err
	}

	// delete temp files
	if err := DeleteFiles(jobname, zipname); err != nil {
		return err
	}

	return nil

}

// Given a project name, this API call fetches all flow ids of that project.
func (this *Client) FetchFlows(project string) (*Flows, error) {

	// init return
	var flows Flows

	// set form parameters
	values := url.Values{}
	values.Add("ajax", "fetchprojectflows")
	values.Add("session.id", this.Session)
	values.Add("project", project)

	// try to get project flows
	err := this.action(http.MethodGet, "/manager", values, &flows)

	// project does not exist
	if err == EmptyResponse {
		err = ProjectNotFound
	}

	return &flows, err

}

// For a given project and a flow id, this API call fetches all the jobs that belong to this flow.
// It also returns the corresponding graph structure of those jobs.
func (this *Client) FetchJobs(project, flow string) (*Jobs, error) {

	// init return
	var jobs Jobs

	// check if project exists
	_, err := this.GetProject(project)

	// project exists
	if err == nil {

		// set form parameters
		values := url.Values{}
		values.Add("ajax", "fetchflowgraph")
		values.Add("session.id", this.Session)
		values.Add("project", project)
		values.Add("flow", flow)

		// try to get project flows
		err = this.action(http.MethodGet, "/manager", values, &jobs)

	}

	return &jobs, err

}

// Given a project name, and a certain flow, this API call provides a list of corresponding executions.
// Those executions are sorted in descendent submit time order.
// Also parameters are expected to specify the start index and the length of the list.
// This is originally used to handle pagination.
func (this *Client) FetchExecutions(project, flow string, start, length int) (*Executions, error) {

	// init return
	var executions Executions

	// check if project exists
	_, err := this.GetProject(project)

	// project exists
	if err == nil {

		// set form parameters
		values := url.Values{}
		values.Add("ajax", "fetchFlowExecutions")
		values.Add("session.id", this.Session)
		values.Add("project", project)
		values.Add("flow", flow)
		values.Add("start", strconv.Itoa(start))
		values.Add("length", strconv.Itoa(length))

		// try to get project flows
		err = this.action(http.MethodGet, "/manager", values, &executions)

	}

	return &executions, err

}

// Given a project name and a flow id, this API call fetches only executions that are currently running.
func (this *Client) FetchRunningExecutions(project, flow string) (*Running, error) {

	// init return
	var running Running

	// check if project exists
	_, err := this.GetProject(project)

	// project exists
	if err == nil {

		// set form parameters
		values := url.Values{}
		values.Add("ajax", "getRunning")
		values.Add("session.id", this.Session)
		values.Add("project", project)
		values.Add("flow", flow)

		// try to get project flows
		err = this.action(http.MethodGet, "/executor", values, &running)

	}

	return &running, err

}

// This API executes a flow via an ajax call, supporting a rich selection of different options.
func (this *Client) ExecuteFlow(project, flow string) (*Execute, error) {

	// init return
	var execute Execute

	// check if project exists
	_, err := this.GetProject(project)

	// project exists
	if err == nil {

		// set form parameters
		values := url.Values{}
		values.Add("ajax", "executeFlow")
		values.Add("session.id", this.Session)
		values.Add("project", project)
		values.Add("flow", flow)

		// try to get project flows
		err = this.action(http.MethodGet, "/executor", values, &execute)

	}

	return &execute, err

}

// Given an execution id, this API call cancels a running flow. If the flow is not running, it will return an error message.
func (this *Client) CancelFlow(project, flow string) error {

	// check if project exists
	_, err := this.GetProject(project)

	// project exists
	if err == nil {

		// set form parameters
		values := url.Values{}
		values.Add("ajax", "cancelFlow")
		values.Add("session.id", this.Session)
		values.Add("project", project)
		values.Add("flow", flow)

		// an empty struct will return if succeeds
		var response map[string]string

		// request api
		if err = this.action(http.MethodGet, "/executor", values, &response); err == nil {

			// check for error
			if message, find := response["error"]; find {
				err = errors.New(message)
			}

		}

	}

	return err

}

// This API call schedules a flow.
func (this *Client) ScheduleFlow(project *Project, flow string, schedule time.Time, repeat, period string) (*Detail, error) {

	// set form parameters
	values := url.Values{}
	values.Add("ajax", "scheduleFlow")
	values.Add("session.id", this.Session)
	values.Add("projectId", strconv.Itoa(project.ID))
	values.Add("projectName", project.Name)
	values.Add("flow", flow)
	values.Add("scheduleTime", fmtdate.Format("hh,mm,pm,ZZZ", schedule))
	values.Add("scheduleDate", fmtdate.Format("MM/DD/YYYY", schedule))
	values.Add("is_recurring", repeat)
	values.Add("period", period)

	// an empty struct will return if succeeds
	var detail Detail

	// request api
	err := this.action(http.MethodPost, "/schedule", values, &detail)

	return &detail, err

}

// This API call unschedules a flow.
func (this *Client) UnscheduleFlow(project *Project, flow string, schedule time.Time, repeat, period string) (*Detail, error) {

	// set form parameters
	values := url.Values{}
	values.Add("ajax", "scheduleFlow")
	values.Add("session.id", this.Session)
	values.Add("projectId", strconv.Itoa(project.ID))
	values.Add("projectName", project.Name)
	values.Add("flow", flow)
	values.Add("scheduleTime", fmtdate.Format("hh,mm,pm,ZZZ", schedule))
	values.Add("scheduleDate", fmtdate.Format("MM/DD/YYYY", schedule))
	values.Add("is_recurring", repeat)
	values.Add("period", period)

	// an empty struct will return if succeeds
	var detail Detail

	// request api
	err := this.action(http.MethodPost, "/schedule", values, &detail)

	return &detail, err

}
