package emr

import "testing"

func TestIsSparkSubmitStep_BareCommand(t *testing.T) {
	if !isSparkSubmitStep([]string{"spark-submit", "--class", "Main", "app.jar"}) {
		t.Fatal("bare spark-submit should be recognised")
	}
}

func TestIsSparkSubmitStep_FullPath(t *testing.T) {
	if !isSparkSubmitStep([]string{"/usr/lib/spark/bin/spark-submit", "app.jar"}) {
		t.Fatal("full path to spark-submit should be recognised")
	}
}

func TestIsSparkSubmitStep_OtherCommand(t *testing.T) {
	if isSparkSubmitStep([]string{"hadoop", "jar", "app.jar"}) {
		t.Fatal("hadoop jar step should not be recognised as spark-submit")
	}
}

func TestIsSparkSubmitStep_EmptyArgv(t *testing.T) {
	if isSparkSubmitStep(nil) || isSparkSubmitStep([]string{}) {
		t.Fatal("empty argv should return false")
	}
}
