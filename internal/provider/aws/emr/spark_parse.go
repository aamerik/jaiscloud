package emr

import (
	"fmt"
	"strings"

	"jaiscloud/internal/sparkhelpers"
)

// extractStepArgv extracts HadoopJarStep.Args as []string from a step config map.
// The first element is conventionally the program name (e.g. "spark-submit").
func extractStepArgv(stepCfg map[string]any) []string {
	hj, ok := stepCfg["HadoopJarStep"].(map[string]any)
	if !ok {
		return nil
	}
	raw, ok := hj["Args"].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, a := range raw {
		if s, ok := a.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// parseSparkSubmitArgv parses the argv that comes AFTER "spark-submit" has been
// stripped and AFTER TranslateEMREC2YarnArgs has been applied.
//
// It scans for the entry-point file (last positional argument with a recognised
// extension: .jar, .py, .r, .R) and splits the list into:
//   - sparkArgs: everything before the entry-point (flags + --conf entries)
//   - userArgs:  everything after the entry-point (application arguments)
//
// For JAR entry-points the --class flag is consumed from sparkArgs.
// For Python entry-points --py-files is consumed from sparkArgs.
func parseSparkSubmitArgv(argv []string) (sparkhelpers.EntryPoint, []string, []string, error) {
	// Find the entry-point position: last arg that looks like a file path.
	epIdx := -1
	for i, arg := range argv {
		if strings.HasPrefix(arg, "-") {
			// It's a flag; skip — but skip its value too if it takes one.
			continue
		}
		if isEntryPointArg(arg) {
			epIdx = i
		}
	}
	if epIdx < 0 {
		return nil, nil, nil, fmt.Errorf("emr: no entry-point (jar/py/R) found in spark-submit args: %v", argv)
	}

	sparkArgs := make([]string, 0, epIdx)
	userArgs := argv[epIdx+1:]
	ep := argv[epIdx]

	// Walk the spark args section collecting flags; extract --class / --py-files.
	mainClass := ""
	var pyFiles []string
	for i := 0; i < epIdx; i++ {
		arg := argv[i]
		if (arg == "--class" || arg == "--main-class") && i+1 < epIdx {
			mainClass = argv[i+1]
			sparkArgs = append(sparkArgs, arg, argv[i+1])
			i++
			continue
		}
		if arg == "--py-files" && i+1 < epIdx {
			pyFiles = strings.Split(argv[i+1], ",")
			sparkArgs = append(sparkArgs, arg, argv[i+1])
			i++
			continue
		}
		sparkArgs = append(sparkArgs, arg)
	}

	var entry sparkhelpers.EntryPoint
	lower := strings.ToLower(ep)
	switch {
	case strings.HasSuffix(lower, ".py"):
		entry = sparkhelpers.PythonEntryPoint{MainPythonFile: ep, PyFiles: pyFiles}
	case strings.HasSuffix(lower, ".r"):
		entry = sparkhelpers.REntryPoint{MainRFile: ep}
	default: // .jar or anything else
		entry = sparkhelpers.JarEntryPoint{JarURI: ep, MainClass: mainClass}
	}

	return entry, sparkArgs, userArgs, nil
}

// isEntryPointArg returns true when s looks like a Spark entry-point file.
func isEntryPointArg(s string) bool {
	lower := strings.ToLower(s)
	return strings.HasSuffix(lower, ".jar") ||
		strings.HasSuffix(lower, ".py") ||
		strings.HasSuffix(lower, ".r") ||
		// Also accept bare s3:// or gs:// URIs ending in a recognised extension.
		(strings.Contains(lower, "://") && (strings.Contains(lower, ".jar") || strings.Contains(lower, ".py")))
}
