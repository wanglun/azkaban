package azkaban

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const AZKABAN_ENDPOINT = "AZKABAN_ENDPOINT"
const AZKABAN_USER = "AZKABAN_USER"
const AZKABAN_PASS = "AZKABAN_PASS"

const PROJECT_NAME = "test"

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

	project, err := client.GetProject(PROJECT_NAME)
	assert.Nil(t, err)
	t.Logf("%#v", project)

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
	execute, err := client.ExecuteFlow(PROJECT_NAME, flow_id)
	assert.Nil(t, err)
	t.Logf("%#v", execute)
}
