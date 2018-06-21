package azkaban

import (
	"archive/zip"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
)

type File struct {
	Name string
	Body []byte
}

func WriteFile(path string, commands ...string) error {

	// create file
	file, err := os.Create(path)

	if err != nil {
		return err
	}

	// close file in the end
	defer file.Close()

	// first line
	if _, err = file.WriteString("type=command\n"); err != nil {
		return err
	}

	// add commands
	for i, cmd := range commands {

		line := "command%s\n"

		if i > 0 {
			line = fmt.Sprintf(line, "."+strconv.Itoa(i)+"="+cmd)
		} else {
			line = fmt.Sprintf(line, "="+cmd)
		}

		if _, err = file.WriteString(line); err != nil {
			return err
		}

	}

	return nil

}

func ReadFiles(list ...string) (files []File, err error) {

	// resize []File
	files = make([]File, len(list))

	// read files
	for i, file := range list {

		// split path from file name
		path := regexp.MustCompile(`\\|/`).Split(file, -1)

		// set file name
		files[i].Name = path[len(path)-1]

		// read file content
		files[i].Body, err = ioutil.ReadFile(file)

	}

	return
}

func ZipFiles(name string, list ...string) error {

	// create a file to write to
	zipFile, err := os.Create(name)

	// close file on defer
	defer zipFile.Close()

	// error creating tar file
	if err != nil {
		return err
	}

	// create a new zip archive
	zipWriter := zip.NewWriter(zipFile)

	// close writer on defer
	defer zipWriter.Close()

	// read the files from disk
	files, err := ReadFiles(list...)

	// error reading files from disk
	if err != nil {
		return err
	}

	// archive files
	for _, file := range files {

		// create entry into zip
		f, err := zipWriter.Create(file.Name)

		// error creating entry
		if err != nil {
			return err
		}

		// write content
		_, err = f.Write([]byte(file.Body))

		// error writing zip
		if err != nil {
			return err
		}
	}

	return nil
}

func DeleteFiles(files ...string) error {

	for _, file := range files {
		if err := os.Remove(file); err != nil {
			return err
		}
	}

	return nil

}
