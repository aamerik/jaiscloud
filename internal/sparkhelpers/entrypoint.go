package sparkhelpers

import "strings"

// EntryPointArgs returns (preJarArgs []string, jarOrScript string) for spark-submit.
//
//   - JAR:    preJarArgs=["--class", MainClass] (empty if no MainClass), jarOrScript=JarURI
//   - Python: preJarArgs=["--py-files", joined(PyFiles)] if PyFiles non-empty, jarOrScript=MainPythonFile
//   - R:      preJarArgs=nil, jarOrScript=MainRFile
func EntryPointArgs(ep EntryPoint) (preJarArgs []string, jarOrScript string) {
	switch e := ep.(type) {
	case JarEntryPoint:
		if e.MainClass != "" {
			preJarArgs = []string{"--class", e.MainClass}
		}
		jarOrScript = e.JarURI
	case PythonEntryPoint:
		if len(e.PyFiles) > 0 {
			preJarArgs = []string{"--py-files", strings.Join(e.PyFiles, ",")}
		}
		jarOrScript = e.MainPythonFile
	case REntryPoint:
		jarOrScript = e.MainRFile
	}
	return
}
