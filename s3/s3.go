package s3

import (
	"bytes"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3Client struct {
	MyBucket string
	Region   string
	Sess     *session.Session
	Svc      *s3.S3
}

func exitErrorf(msg string, args ...interface{}) {
	log.Printf(msg + "\n")
	os.Exit(1)
}
func (t *S3Client) NewSession(region string) {
	t.Region = region
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(t.Region)},
	)
	if err != nil {
		exitErrorf("PROBLEMA DE SESSION CON S3, %v", err)
	}
	t.Sess = sess
	t.Svc = s3.New(t.Sess)
}
func (t *S3Client) Ls() *s3.ListBucketsOutput {
	result, err := t.Svc.ListBuckets(nil)
	if err != nil {
		exitErrorf("Unable to list buckets, %v", err)
	}
	return result
}
func (t *S3Client) LsPrint() {
	result := t.Ls()
	for _, b := range result.Buckets {
		log.Printf("* %s created on %s\n",
			aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
	}
}
func (t *S3Client) AddFilesToS3(file io.Reader, size int64, myBucket string, fileDir string) error {

	//file, err := os.Open(origin)
	/*if err != nil {
		return err
	}
	defer file.Close()*/

	//fileInfo, _ := file.Stat()
	//var size int64 = fileInfo.Size()
	buffer := make([]byte, size)
	file.Read(buffer)

	_, err := t.Svc.PutObject(&s3.PutObjectInput{
		Bucket:        aws.String(myBucket),
		Key:           aws.String(fileDir),
		ACL:           aws.String("private"),
		Body:          bytes.NewReader(buffer),
		ContentLength: aws.Int64(size),
		ContentType:   aws.String("image/jpeg"),
	})
	return err

}
func (t *S3Client) Upload(filename string, myBucket string, keyName string, ContentType string) error {
	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(t.Sess)

	f, err := os.Open(filename)
	if err != nil {
		log.Printf("failed to open file %q, %v", filename, err)
		return err
	}

	// Upload the file to S3.
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:      aws.String(myBucket),
		Key:         aws.String(keyName),
		Body:        f,
		ContentType: aws.String(ContentType),
	})
	if err != nil {
		log.Printf("failed to upload file, %v", err)
		return err
	}
	log.Println(result)
	return nil
}
func (t *S3Client) GenerateUrlForDownload(myBucket string, keyName string) string {

	req, _ := t.Svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(myBucket),
		Key:    aws.String(keyName),
	})
	urlStr, err := req.Presign(15 * time.Minute)

	if err != nil {
		log.Println("Failed to sign request", err)
	}
	return urlStr

}
func (t *S3Client) DeleteObject(myBucket string, keyName string) {

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(myBucket),
		Key:    aws.String(keyName),
	}

	result, err := t.Svc.DeleteObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				log.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			log.Println(err.Error())
		}
		return
	}
	log.Println(result)
}
func (t *S3Client) GetObject(bucket string, key string) ([]byte, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket), // Required
		Key:    aws.String(key),    // Required
	}
	resp, err := t.Svc.GetObject(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		log.Println(err.Error())
		return nil, err
	} else {
		size := int(*resp.ContentLength)

		buffer := make([]byte, size)
		defer resp.Body.Close()
		//var bbuffer bytes.Buffer
		//for true {
		//num, rerr := resp.Body.Read(buffer)
		resp.Body.Read(buffer)
		//	if num > 0 {
		//		bbuffer.Write(buffer[:num])
		//	} else if rerr == io.EOF || rerr != nil {
		//		break
		//	}
		//}
		//return bbuffer
		return buffer, nil

	}

}
