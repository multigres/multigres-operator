package manager

import (
	"errors"
	"io"
	"os"
	"testing"

	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/multigres/testkit/assert"
)

func TestControllerManagerUsesNonOverlappingRollout(t *testing.T) {
	c := assert.NewCollecting(t)
	f, err := os.Open("manager.yaml")
	c.Require().NoError(err)
	defer func() {
		c.NoError(f.Close(), "close manager manifest")
	}()

	type manifest struct {
		Kind string `json:"kind"`
		Spec struct {
			Strategy map[string]any `json:"strategy"`
		} `json:"spec"`
	}

	decoder := yaml.NewYAMLOrJSONDecoder(f, 4096)
	for {
		var resource manifest
		if err := decoder.Decode(&resource); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatal(err)
		}
		if resource.Kind != "Deployment" {
			continue
		}
		got := resource.Spec.Strategy["type"]
		c.Require().False(got != "Recreate", "controller-manager strategy = %q, want Recreate", got)
		rollingUpdate, present := resource.Spec.Strategy["rollingUpdate"]
		c.Require().True(present, "controller-manager strategy must explicitly clear rollingUpdate")
		c.Require().Nil(rollingUpdate, "controller-manager rollingUpdate")
		return
	}

	t.Fatal("controller-manager Deployment not found")
}
