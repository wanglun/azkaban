package azkaban

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const AZKABAN_ENDPOINT = "AZKABAN_ENDPOINT"
const AZKABAN_USER = "AZKABAN_USER"
const AZKABAN_PASS = "AZKABAN_PASS"

const PROJECT_NAME = "test_client"
const PROJECT_DESC = "project for client testing"
const FLOW_ZIP_PATH = "./testdata/testflow.zip"

func TestClient(t *testing.T) {
	endpoint := os.Getenv(AZKABAN_ENDPOINT)
	user := os.Getenv(AZKABAN_USER)
	pass := os.Getenv(AZKABAN_PASS)
	if endpoint == "" || user == "" || pass == "" {
		t.Fatal("endpoint/user/pass is empty")
	}

	client := New(endpoint)
	err := client.Authenticate(user, pass)
	assert.Nil(t, err)

	// create project
	_, err = client.GetProject(PROJECT_NAME)
	if err == nil {
		_, err = client.DeleteProject(PROJECT_NAME)
		assert.Nil(t, err)
	}
	_, err = client.CreateProject(PROJECT_NAME, PROJECT_DESC)
	assert.Nil(t, err)

	// upload flow
	err = client.UploadProjectZip(PROJECT_NAME, FLOW_ZIP_PATH)
	assert.Nil(t, err)

	// flows
	flows, err := client.FetchFlows(PROJECT_NAME)
	assert.Nil(t, err)
	t.Logf("%#v", flows)
	assert.True(t, len(flows.Flows) > 0)
	flow_id := flows.Flows[0].IdFlow

	// jobs
	jobs, err := client.FetchJobs(PROJECT_NAME, flow_id)
	assert.Nil(t, err)
	t.Logf("%#v", jobs)

	// execute flow
	execute, err := client.ExecuteFlow(PROJECT_NAME, flow_id, ConcurrentOptionDefault, nil)
	assert.Nil(t, err)
	t.Logf("%#v", execute)

	// execute flow with override properties
	execute, err = client.ExecuteFlow(PROJECT_NAME, flow_id, ConcurrentOptionIgnore, map[string]string{"test.p1": "100", "test.p2": "p2_overrided"})
	assert.Nil(t, err)
	t.Logf("%#v", execute)
}
