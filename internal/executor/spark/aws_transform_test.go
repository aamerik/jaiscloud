package spark

import (
	"strings"
	"testing"
)

func TestAWSResolveSparkCommand_Pattern1_EMRContainers(t *testing.T) {
	// Pattern 1: absolute path entry point — args pass through unchanged (no rewriting).
	job := SparkJob{
		JobID:  "jr-001",
		JarURI: "/opt/spark/bin/spark-submit",
		Args: []string{
			"--master", "yarn",
			"--class", "org.apache.spark.examples.SparkPi",
			"s3://bucket/app.jar",
			"10",
		},
	}
	cfg := SparkConfigFrom("k8s", SizeSmall)

	cmd, err := AWSResolveSparkCommand(job, cfg)
	if err != nil { t.Fatal(err) }

	if cmd.Binary != "/opt/spark/bin/spark-submit" {
		t.Errorf("binary: got %q, want /opt/spark/bin/spark-submit", cmd.Binary)
	}
	// Args pass through unchanged — caller supplies --master; no rewriting.
	assertMaster(t, cmd.Args, "yarn")
}

func TestAWSResolveSparkCommand_Pattern2_CommandRunner(t *testing.T) {
	// Pattern 2: command-runner.jar — args pass through after extracting the binary.
	job := SparkJob{
		JobID:  "s-001",
		JarURI: "command-runner.jar",
		Args: []string{
			"spark-submit",
			"--master", "yarn",
			"--class", "org.apache.spark.examples.SparkPi",
			"s3://bucket/app.jar",
			"10",
		},
	}
	cfg := SparkConfigFrom("k8s", SizeSmall)

	cmd, err := AWSResolveSparkCommand(job, cfg)
	if err != nil { t.Fatal(err) }

	if cmd.Binary != "/opt/spark/bin/spark-submit" {
		t.Errorf("binary: got %q, want /opt/spark/bin/spark-submit", cmd.Binary)
	}
	// "spark-submit" must be extracted from args, not left as args[0]
	if len(cmd.Args) > 0 && cmd.Args[0] == "spark-submit" {
		t.Errorf("spark-submit binary should not remain as args[0], args=%v", cmd.Args)
	}
	// Args pass through unchanged — caller supplies --master; no rewriting.
	assertMaster(t, cmd.Args, "yarn")
}

func TestAWSResolveSparkCommand_Pattern3_RealJar(t *testing.T) {
	// Pattern 3: real JAR — SparkSubmitArgs builds full spark-submit invocation with k8s:// master.
	cfg := SparkConfigFrom("k8s", SizeSmall)
	cfg.Image = "apache/spark:3.5.0"
	job := SparkJob{
		JobID:     "j-001",
		JarURI:    "s3://bucket/app.jar",
		MainClass: "com.example.Main",
		Config:    cfg,
	}

	cmd, err := AWSResolveSparkCommand(job, cfg)
	if err != nil { t.Fatal(err) }

	if cmd.Binary != "/opt/spark/bin/spark-submit" {
		t.Errorf("binary: got %q, want /opt/spark/bin/spark-submit", cmd.Binary)
	}
	// SparkSubmitArgs adds --master k8s://... when mode=k8s.
	master, ok := findMasterArg(cmd.Args)
	if !ok || !strings.HasPrefix(master, "k8s://") {
		t.Errorf("expected --master k8s://... in args, got %q (args: %v)", master, cmd.Args)
	}
}

func TestAWSResolveSparkCommand_StripIncompatibleConfs(t *testing.T) {
	job := SparkJob{
		JobID:  "j-strip",
		JarURI: "command-runner.jar",
		Args: []string{
			"spark-submit",
			"--conf", "spark.extraListeners=com.example.Listener",
			"--conf", "spark.sql.extensions=com.example.Extension",
			"--conf", "spark.executor.memory=2g",
			"s3://bucket/app.jar",
		},
	}
	cfg := SparkConfigFrom("k8s", SizeSmall)
	cfg.S3Endpoint = "http://jaiscloud:4566"
	cfg.Region = "us-east-1"
	cfg.AWSAccessKey = "test"
	cfg.AWSSecretKey = "test"

	cmd, err := AWSResolveSparkCommand(job, cfg)
	if err != nil { t.Fatal(err) }

	for i, a := range cmd.Args {
		if a == "--conf" && i+1 < len(cmd.Args) {
			val := cmd.Args[i+1]
			if strings.HasPrefix(val, "spark.extraListeners=") {
				t.Errorf("spark.extraListeners should be stripped, found: %q", val)
			}
			if strings.HasPrefix(val, "spark.sql.extensions=") {
				t.Errorf("spark.sql.extensions should be stripped, found: %q", val)
			}
		}
	}
	// spark.executor.memory must survive
	found := false
	for i, a := range cmd.Args {
		if a == "--conf" && i+1 < len(cmd.Args) && strings.HasPrefix(cmd.Args[i+1], "spark.executor.memory=") {
			found = true
		}
	}
	if !found {
		t.Error("spark.executor.memory should remain in args after stripping")
	}
}

func TestAWSResolveSparkCommand_NoS3InjectionWhenEndpointEmpty(t *testing.T) {
	job := SparkJob{
		JobID:  "j-nos3",
		JarURI: "command-runner.jar",
		Args:   []string{"spark-submit", "s3://bucket/app.jar"},
	}
	cfg := SparkConfigFrom("k8s", SizeSmall)
	// S3Endpoint intentionally empty

	cmd, err := AWSResolveSparkCommand(job, cfg)
	if err != nil { t.Fatal(err) }

	for i, a := range cmd.Args {
		if a == "--conf" && i+1 < len(cmd.Args) {
			v := cmd.Args[i+1]
			if strings.Contains(v, "s3.endpoint") || strings.Contains(v, "s3a.endpoint") {
				t.Errorf("unexpected S3 endpoint conf when S3Endpoint is empty: %q", v)
			}
		}
	}
}

func TestAWSResolveSparkCommand_MissingMaster_PassesThrough(t *testing.T) {
	// Pattern 2 without --master: args pass through; no master is prepended.
	job := SparkJob{
		JobID:  "j-nomaster",
		JarURI: "command-runner.jar",
		Args:   []string{"spark-submit", "--class", "com.example.Main", "s3://bucket/app.jar"},
	}
	cfg := SparkConfigFrom("k8s", SizeSmall)

	cmd, err := AWSResolveSparkCommand(job, cfg)
	if err != nil { t.Fatal(err) }

	// No --master was supplied and args pass through without rewriting.
	for i, a := range cmd.Args {
		if a == "--master" && i+1 < len(cmd.Args) {
			t.Errorf("unexpected --master %q in args: %v", cmd.Args[i+1], cmd.Args)
		}
	}
}

func TestAWSResolveSparkCommand_S3InjectionPresent(t *testing.T) {
	job := SparkJob{
		JobID:  "j-s3inject",
		JarURI: "command-runner.jar",
		Args:   []string{"spark-submit", "s3://bucket/app.jar"},
	}
	cfg := SparkConfigFrom("k8s", SizeSmall)
	cfg.S3Endpoint = "http://minio:9000"
	cfg.AWSAccessKey = "minioadmin"
	cfg.AWSSecretKey = "minioadmin"

	cmd, err := AWSResolveSparkCommand(job, cfg)
	if err != nil { t.Fatal(err) }

	found := false
	for i, a := range cmd.Args {
		if a == "--conf" && i+1 < len(cmd.Args) && strings.HasPrefix(cmd.Args[i+1], "spark.hadoop.fs.s3a.endpoint=") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected S3A endpoint conf to be injected, args=%v", cmd.Args)
	}
}

// assertMaster checks that --master <want> appears in args.
func assertMaster(t *testing.T, args []string, want string) {
	t.Helper()
	for i, a := range args {
		if a == "--master" && i+1 < len(args) {
			if args[i+1] != want {
				t.Errorf("--master: got %q, want %q", args[i+1], want)
			}
			return
		}
	}
	t.Errorf("--master not found in args: %v", args)
}
