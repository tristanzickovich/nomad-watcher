package main

import (
    "os"
    "fmt"
    "syscall"
    "encoding/json"
    "path/filepath"
    "io"
    "archive/zip"

    flags "github.com/jessevdk/go-flags"
    log "github.com/Sirupsen/logrus"

    nomad "github.com/hashicorp/nomad/api"

    "github.com/bsd/nomad-watcher/watcher"
    "github.com/kjk/dailyrotate"
)

var (
    version string = "undef"
    rotatedFile *dailyrotate.File
    rotatedFileName string
)

type Options struct {
    Debug bool       `env:"DEBUG"      long:"debug"      description:"enable debug"`
    LogRotate bool   `env:"LOG_ROTATE" long:"log-rotate" description:"enable log rotation"`
    LogFile string   `env:"LOG_FILE"   long:"log-file"   description:"path to JSON log file"`
    EventFile string `env:"EVENT_FILE" long:"event-file" description:"path to JSON event file" required:"true"`
}

func rotatingFileClosed(path string, didRotate bool) {
    fmt.Printf("we just closed a file '%s', didRotate: %v\n", path, didRotate)
    if !didRotate {
        return
    }
    go func() {
        log.Info("This is where to compress and email the log files")
        err := ZipFiles("output", path)
        if err != nil {
            log.Fatal(err)
        }
        fmt.Println("Zipped File: " + "output")
    }()
}

func initRotatedFile() string{
    var err error
    rotatedFilePath := filepath.Join("rotated-logs", "2006-01-02.log")
    rotatedFile, err = dailyrotate.NewFile(rotatedFilePath, rotatingFileClosed)
    _,err  = io.WriteString(rotatedFile,"")
    if err != nil {
      log.Panic("err: %s", err)
    }
    return rotatedFilePath
}

func main() {
    var opts Options

    _, err := flags.Parse(&opts)
    if err != nil {
        os.Exit(1)
    }

    if opts.Debug {
        log.SetLevel(log.DebugLevel)
    }

    if opts.LogFile != "" {
        logFp, err := os.OpenFile(opts.LogFile, os.O_WRONLY | os.O_APPEND | os.O_CREATE, 0600)
        checkError(fmt.Sprintf("error opening %s", opts.LogFile), err)

        defer logFp.Close()

        // ensure panic output goes to log file
        syscall.Dup2(int(logFp.Fd()), 1)
        syscall.Dup2(int(logFp.Fd()), 2)

        // log as JSON
        log.SetFormatter(&log.JSONFormatter{})

        // send output to file
        log.SetOutput(logFp)
    }

    if opts.LogRotate {
        log.Info("Log Rotation enabled, please see /rotated-logs/<<date>> for most recent logs" )
        rotatedFileName = initRotatedFile()
        log.SetOutput(rotatedFile)
    } else {
        rotatedFileName = opts.EventFile
    }

    evtsFp, err := os.OpenFile(rotatedFileName, os.O_WRONLY | os.O_APPEND | os.O_CREATE, 0600)
    checkError(fmt.Sprintf("error opening %s", rotatedFileName), err)
    defer evtsFp.Close()

    nomadClient, err := nomad.NewClient(nomad.DefaultConfig())
    checkError("creating Nomad client", err)

    enc := json.NewEncoder(rotatedFile)

    eventChan := make(chan interface{})

    allocEventChan, taskStateEventChan := watcher.WatchAllocations(nomadClient.Allocations())
    go func() {
        for ae := range allocEventChan {
            eventChan <- ae
        }
    }()

    go func() {
        for tse := range taskStateEventChan {
            eventChan <- tse
        }
    }()

    go func() {
        for ee := range watcher.WatchEvaluations(nomadClient.Evaluations()) {
            eventChan <- ee
        }
    }()

    go func() {
        for je := range watcher.WatchJobs(nomadClient.Jobs()) {
            eventChan <- je
        }
    }()

    go func() {
        for ne := range watcher.WatchNodes(nomadClient.Nodes()) {
            eventChan <- ne
        }
    }()

    for e := range eventChan {
        log.Info("HERE", e)
        checkError("serializing event", enc.Encode(e))
    }
}

func ZipFiles(filename string, file string) error {

    newfile, err := os.Create(filename)
    if err != nil {
        return err
    }
    defer newfile.Close()

    zipWriter := zip.NewWriter(newfile)
    defer zipWriter.Close()

    zipfile, err := os.Open(file)
    if err != nil {
        return err
    }
    defer zipfile.Close()

    info, err := zipfile.Stat()
    if err != nil {
        return err
    }

    header, err := zip.FileInfoHeader(info)
    if err != nil {
        return err
    }

    header.Method = zip.Deflate

    writer, err := zipWriter.CreateHeader(header)
    if err != nil {
        return err
    }
    _, err = io.Copy(writer, zipfile)
    if err != nil {
        return err
    }
    return nil
}
