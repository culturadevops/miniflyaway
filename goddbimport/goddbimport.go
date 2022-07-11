package goddbimport

import (
	"bytes"
	"fmt"
	"os/exec"
	"path/filepath"
)

type DDbimportObj struct {
	Table string
	File  string
}

func SetVar(table string, file string) DDbimportObj {
	return DDbimportObj{
		Table: table,
		File:  file,
	}
}
func (j *DDbimportObj) Tesst() *exec.Cmd {
	cmd := exec.Command("./ddbimport")
	return cmd
}
func (j *DDbimportObj) Command(arg ...string) *exec.Cmd {

	absPath, _ := filepath.Abs("./")

	arg = append([]string{"-inputFile", absPath + j.File, "-delimiter", "tab",
		"-numericFields", "year",
		"-tableRegion", "us-east-1",
		"-tableName", j.Table,
	}, arg...)
	cmd := exec.Command("./ddbimport", arg...)
	return cmd
}

func (j *DDbimportObj) UpTable(table string, file string) error {
	j.Table = table
	j.File = file
	cmd := j.Command()

	//absPath, _ := filepath.Abs("./")
	//cmd.Dir = absPath
	fmt.Println(cmd)
	return MyRun(cmd, true)

}
func MyRun(cmd *exec.Cmd, flagStdOut bool) error {
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	outStr, errStr := string(stdout.Bytes()), string(stderr.Bytes())
	if flagStdOut {
		fmt.Printf("out:\n%s\n", outStr)
	}
	fmt.Printf("err:\n%s\n", errStr)
	return err
}
