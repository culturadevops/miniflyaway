package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/culturadevops/mfw/goddbimport"
	"github.com/culturadevops/mfw/s3"
)

type FilesStruct struct {
	Number int
	Name   string
	Path   string
	Sum    string
	Ready  string
}
type misfiles struct {
	Folder string
	Files  []FilesStruct
}
type items struct {
	items []misfiles
}

type ArchivosExistente struct {
	Encontrados []misfiles
}

func SearchIntoTheFolder(filesencontrados []FilesStruct, filesinstatus []FilesStruct) []FilesStruct {
	boolencontrado := false
	var FilesOrdenados []FilesStruct
	for _, onefilesencontrado := range filesencontrados {
		boolencontrado = false
		for _, onefilesstatus := range filesinstatus {
			if onefilesencontrado.Number == onefilesstatus.Number {
				boolencontrado = true
				//if onefilesstatus.Ready == "noReady" {
				FilesOrdenados = append(FilesOrdenados, onefilesstatus)
				//}
			}
		}
		if boolencontrado == false {
			FilesOrdenados = append(FilesOrdenados, onefilesencontrado)
		}
	}
	return FilesOrdenados
}
func GetfilesforFolder(i int, Encontrado []misfiles, InStatus []misfiles) (misfiles, misfiles) {
	var encontrado misfiles
	var instatus misfiles
	for iencontrado, folder := range Encontrado {
		if i == iencontrado {
			encontrado = folder
		}
	}
	for iinstatus, folderinstatus := range InStatus {
		if i == iinstatus {
			instatus = folderinstatus
		}
	}
	return encontrado, instatus
}
func CreateExecuteList(Encontrado []misfiles, InStatus []misfiles) []misfiles {
	var FolderOrdenados []misfiles

	for i, folder := range Encontrado {

		filesencontrados, filesstatus := GetfilesforFolder(i, Encontrado, InStatus)
		FilesOrdenados := SearchIntoTheFolder(filesencontrados.Files, filesstatus.Files)
		FolderToSave := misfiles{
			Folder: folder.Folder,
			Files:  FilesOrdenados,
		}
		FolderOrdenados = append(FolderOrdenados, FolderToSave)
	}
	return FolderOrdenados
}

func Ordenarfiles(filesEncontrado []FilesStruct, Listafinal []int) []FilesStruct {
	var FilesOrdenados []FilesStruct
	for _, valor := range Listafinal {
		for _, estructura := range filesEncontrado {

			if valor == estructura.Number {

				FilesOrdenados = append(FilesOrdenados, estructura)
			}
		}
	}
	return FilesOrdenados
}
func Burbuja(ListaDesordenada []int) []int {
	var auxiliar int
	for i := 0; i < len(ListaDesordenada); i++ {
		for j := 0; j < len(ListaDesordenada); j++ {
			if ListaDesordenada[i] < ListaDesordenada[j] {
				auxiliar = ListaDesordenada[i]
				ListaDesordenada[i] = ListaDesordenada[j]
				ListaDesordenada[j] = auxiliar
			}
		}
	}
	return ListaDesordenada
}
func RemoveDir(filename string) string {

	index := strings.Index(filename, "./")

	leng := len(filename)

	if index > -1 {

		return filename[index+2 : leng]
	}
	return filename
}
func RemovePathForDir(filename string, path string) string {

	index := strings.Index(filename, path)

	leng := len(filename)

	if index > -1 {

		return filename[index+1 : leng]
	}
	return filename
}
func GetNumberFileName(filename string, path string) int {
	index := strings.Index(filename, "_")
	if index > 0 {
		firstCharacter := filename[0:index]
		if firstCharacterint, err := strconv.Atoi(firstCharacter); err == nil {
			return firstCharacterint
		}
	}
	fmt.Printf("Folder:(%q)\nFile:(%q) this file does not start with number.\n", path, filename)
	os.Exit(1)
	return -1
}
func Getchecksum(fileName string) string {

	f, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}
	return string(h.Sum(nil))

}

func (mf *ArchivosExistente) MySearchFiles(rutaFinal string) {
	//fmt.Println(mf.count)
	//fmt.Println(path)
	if _, err := os.Stat(rutaFinal); !os.IsNotExist(err) {

		files, err := ioutil.ReadDir(rutaFinal)
		if err != nil {
			log.Fatal(err)
		}
		var NumberFileForSave int

		var filesEncontrado []FilesStruct
		var ListaDesordenada []int
		for _, f := range files {
			NumberFileForSave++
			path := rutaFinal + "/" + f.Name()
			if f.IsDir() {

				mf.MySearchFiles(path)

			} else {
				if filepath.Ext(f.Name()) == ".csv" {
					//checksum := Getchecksum(path)
					ItemForSave := FilesStruct{
						Number: GetNumberFileName(f.Name(), rutaFinal),
						Name:   f.Name(),
						Path:   RemoveDir(path),
						//Sum:    checksum,
						Ready: "noReady",
					}
					ListaDesordenada = append(ListaDesordenada, GetNumberFileName(f.Name(), rutaFinal))
					filesEncontrado = append(filesEncontrado, ItemForSave)
				}
			}
		}
		Listafinal := Burbuja(ListaDesordenada)
		FilesOrdenados := Ordenarfiles(filesEncontrado, Listafinal)
		FolderToSave := misfiles{
			Folder: RemoveDir(rutaFinal),
			Files:  FilesOrdenados,
		}
		mf.Encontrados = append(mf.Encontrados, FolderToSave)
	}
}

func UploadMultipartformToS3(filename string, bucket string, RutaFinalEnS3 string) (string, error) {
	s3 := new(s3.S3Client)
	s3.NewSession("us-east-1")
	RutaFinalEnS3 = RutaFinalEnS3 + filename
	return RutaFinalEnS3, s3.Upload(filename, bucket, RutaFinalEnS3, "text/plain")

}
func writeFile(filename string, jsoncontent interface{}) {
	file, _ := json.MarshalIndent(jsoncontent, "", " ")
	_ = ioutil.WriteFile(filename, file, 0644)
}

//"github.com/culturadevops/jgt/jio"
func main() {
	var statusfile string
	var buckets3 string
	statusfile = "miniflywaystatus.json"
	buckets3 = "lima-aws-cicd-pipeline"

	s3 := new(s3.S3Client)
	s3.NewSession("us-east-1")
	//s3.LsPrint()
	//tratamdo de traer archivo
	fmt.Println("Buscando archivo s3")
	jsoncontent, err := s3.GetObject(buckets3, statusfile)

	if err == nil {
		fmt.Println("encontrado")
		var nextgeneration []misfiles
		json.Unmarshal([]byte(jsoncontent), &nextgeneration)
		writeFile(statusfile, nextgeneration)
	} else {
		fmt.Println("no encontrado")
		fmt.Println(err)
	}

	rutaFinal := "./loadData"
	f := &ArchivosExistente{
		Encontrados: []misfiles{},
	}
	//var count *int
	fmt.Println("revisando carpeta de Data")
	f.MySearchFiles(rutaFinal)
	for _, value := range f.Encontrados {
		fmt.Println(value)
	}
	fmt.Println("Finalizado")

	jsonFile, err := os.Open(statusfile)
	if err != nil {
		if err.Error() == "open "+statusfile+": no such file or directory" {
			fmt.Println("no se encontro el archivo de configuracion, se procede a crear uno")
			writeFile(statusfile, f.Encontrados)
			jsonFile, err = os.Open(statusfile)
		}
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)
	var MFWStatus []misfiles
	json.Unmarshal([]byte(byteValue), &MFWStatus)
	fmt.Println(MFWStatus)
	fmt.Println("Creando lista para ejecutar")
	executeList := CreateExecuteList(f.Encontrados, MFWStatus)
	//writeFile(statusfile, executeList)
	fmt.Println("-------------------------------")
	fmt.Println(executeList)
	fmt.Println("-------------------------------")
	fmt.Println("Iniciando la ejecucion")
	for _, folder := range executeList {
		fmt.Println("----------------cargando data---------------")
		fmt.Println("-------------------------------")
		for id, file := range folder.Files {
			//for id, file := range folder.Files {
			if folder.Files[id].Ready != "Ready" {
				execute := goddbimport.SetVar(folder.Folder, file.Path)
				tableNamedynamo := RemovePathForDir(folder.Folder, "/")
				fmt.Println("tabla a cargar " + tableNamedynamo)
				fmt.Println("archivo a cargar " + file.Path)
				actualizaarchivoerror := execute.UpTable(tableNamedynamo, "/"+file.Path)
				if actualizaarchivoerror == nil {
					folder.Files[id].Ready = "Ready"
				} else {
					fmt.Println(actualizaarchivoerror)
					writeFile(statusfile, executeList)
					fmt.Println(UploadMultipartformToS3(statusfile, buckets3, ""))
					os.Exit(1)
				}
			} else {
				fmt.Println("Archivo encontrado con estado Ready " + file.Path + " no sera proceso en esta iteracion")
			}

			//
			//fmt.Println("ddbimport -inputFile %q -delimiter tab -numericFields year -tableRegion eu-west-2 -tableName %q", file.Path, folder.Folder)
		}
		writeFile(statusfile, executeList)
		fmt.Println(UploadMultipartformToS3(statusfile, buckets3, ""))
	}
	fmt.Println("RESULTADO DE LA EJECUCION")
	fmt.Println("-------------------------------")
	fmt.Println(executeList)
	fmt.Println("-------------------------------")
	/*for id, _ := range executeList {
		fmt.Println(executeList[id].Folder)
		files := executeList[id].Files
		for idfile, _ := range files {
			fmt.Println(files[idfile].Name)
			fmt.Println(files[idfile].Path)
			fmt.Println(files[idfile].Number)
			fmt.Println(files[idfile].Ready)

		}

	}
	*/
}
