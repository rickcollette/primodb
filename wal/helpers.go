package wal
import (
	"fmt"
	"hash/crc32"
	"os"
	"strings"
	"syscall"
	"github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/credentials"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
    "github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/rickcollette/primodb/config"
)
const (
	fileOpenMode = 0600
	fileOpenFlag = os.O_RDWR
)
var crcTable = crc32.MakeTable(crc32.Castagnoli)
// CalculateHash returns the crc32 value for data
func CalculateHash(data []byte) uint32 {
	h := crc32.New(crcTable)
	h.Write(data)
	return h.Sum32()
}
// Exists function checks if the given path is valid
func Exists(path string) bool {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	} else if err != nil {
		return false
	}
	return true
}
func parseWalName(str string) (seq int64, err error) {
	if !strings.HasSuffix(str, ".wal") {
		return 0, ErrBadWalName
	}
	_, err = fmt.Sscanf(str, "%016x.wal", &seq)
	return seq, err
}
// Fsync full file sync to flush data on disk from temporary buffer
func Fsync(f *os.File) (err error) {
	err = f.Sync()
	return err
}
func fileLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, fileOpenFlag, fileOpenMode)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	// NOTE: calling function should close this file.
	return f, nil
}
// validSeq checks if seq is a monotonically increasing sequence
func (w *Wal) validSeq(seq int64) bool {
	return seq == w.nextseq()
}
func (r *Record) validHash() bool {
	return r.Hash == CalculateHash(r.Data)
}
// Return the latest file based on name
func latestFile(dirPath string, ext string) (os.FileInfo, error) {
    entries, err := os.ReadDir(dirPath)
    if err != nil {
        return nil, err
    }

    var latestWal os.FileInfo
    for i := len(entries) - 1; i >= 0; i-- {
        entry := entries[i]
        if entry.IsDir() {
            continue
        }

        if strings.HasSuffix(entry.Name(), ext) {
            info, err := entry.Info()
            if err != nil {
                return nil, err
            }
            latestWal = info
            break
        }
    }

    return latestWal, nil
}

// InitializeS3Session initializes an S3 session
func InitializeS3Session(config config.S3Config) (*s3manager.Uploader, *s3manager.Downloader) {
    sess := session.Must(session.NewSession(&aws.Config{
        Region:      aws.String(config.Region),
        Credentials: credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, ""),
    }))
    uploader := s3manager.NewUploader(sess)
    downloader := s3manager.NewDownloader(sess)
    return uploader, downloader
}
// UploadToS3 uploads a file to S3
func UploadToS3(uploader *s3manager.Uploader, bucket, key, filePath string) error {
    file, err := os.Open(filePath)
    if err != nil {
        return err
    }
    defer file.Close()
    _, err = uploader.Upload(&s3manager.UploadInput{
        Bucket: aws.String(bucket),
        Key:    aws.String(key),
        Body:   file,
    })
    return err
}
// DownloadFromS3 downloads a file from S3
func DownloadFromS3(downloader *s3manager.Downloader, bucket, key, destPath string) error {
    file, err := os.Create(destPath)
    if err != nil {
        return err
    }
    defer file.Close()
    _, err = downloader.Download(file, &s3.GetObjectInput{
        Bucket: aws.String(bucket),
        Key:    aws.String(key),
    })
    return err
}
