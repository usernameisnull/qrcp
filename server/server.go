package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"

	"github.com/claudiodangelis/qrcp/config"
	"github.com/claudiodangelis/qrcp/pages"
	"github.com/claudiodangelis/qrcp/payload"
	"github.com/claudiodangelis/qrcp/util"
	"gopkg.in/cheggaaa/pb.v1"
)

// Server is the server
type Server struct {
	// SendURL is the URL used to send the file
	SendURL string
	// ReceiveURL is the URL used to Receive the file
	ReceiveURL  string
	instance    *http.Server
	payload     payload.Payload
	outputDir   string
	stopChannel chan bool
	// expectParallelRequests is set to true when qrcp sends files, in order
	// to support downloading of parallel chunks
	expectParallelRequests bool
}

// ReceiveTo sets the output directory
func (s *Server) ReceiveTo(dir string) error {
	output, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	// Check if the output dir exists
	fileinfo, err := os.Stat(output)
	if err != nil {
		return err
	}
	if !fileinfo.IsDir() {
		return fmt.Errorf("%s is not a valid directory", output)
	}
	s.outputDir = output
	return nil
}

// Send adds a handler for sending the file
func (s *Server) Send(p payload.Payload) {
	s.payload = p
	s.expectParallelRequests = true
}

// Wait for transfer to be completed, it waits forever if kept awlive
func (s Server) Wait() error {
	<-s.stopChannel
	log.Println("收到停止信号, 进程将要退出了")
	if err := s.instance.Shutdown(context.Background()); err != nil {
		log.Println(err)
	}
	if s.payload.DeleteAfterTransfer {
		s.payload.Delete()
	}
	return nil
}

// New instance of the server
func New(cfg *config.Config) (*Server, error) {
	app := &Server{}
	// Get the address of the configured interface to bind the server to
	bind, err := util.GetInterfaceAddress(cfg.Interface)
	if err != nil {
		return &Server{}, err
	}
	// Create a listener. If `port: 0`, a random one is chosen
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", bind, cfg.Port))
	if err != nil {
		return nil, err
	}
	// Set the value of computed port
	port := listener.Addr().(*net.TCPAddr).Port
	// Set the host
	host := fmt.Sprintf("%s:%d", bind, port)
	// Get a random path to use
	path := cfg.Path
	if path == "" {
		path = util.GetRandomURLPath()
	}
	// Set the hostname
	hostname := fmt.Sprintf("%s:%d", bind, port)
	// Use external IP when using `interface: any`, unless a FQDN is set
	if bind == "0.0.0.0" && cfg.FQDN == "" {
		fmt.Println("Retrieving the external IP...")
		extIP, err := util.GetExernalIP()
		if err != nil {
			panic(err)
		}
		hostname = fmt.Sprintf("%s:%d", extIP.String(), port)
	}
	// Use a fully-qualified domain name if set
	if cfg.FQDN != "" {
		hostname = fmt.Sprintf("%s:%d", cfg.FQDN, port)
	}
	// Set send and receive URLs
	app.SendURL = fmt.Sprintf("http://%s/send/%s",
		hostname, path)
	app.ReceiveURL = fmt.Sprintf("http://%s/receive/%s",
		hostname, path)
	// Create a server
	httpserver := &http.Server{Addr: host}
	// Create channel to send message to stop server
	app.stopChannel = make(chan bool)
	// Create cookie used to verify request is coming from first client to connect
	cookie := http.Cookie{Name: "qrcp", Value: ""}
	// Gracefully shutdown when an OS signal is received
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		app.stopChannel <- true
		log.Println("os.Interrupt,发送停止信号")
	}()

	var initCookie sync.Once
	// Create handlers
	// Send handler (sends file to caller)
	http.HandleFunc("/send/"+path, sendHandler(cookie,initCookie,app,cfg))
	// Upload handler (serves the upload page)
	http.HandleFunc("/receive/"+path, receiveHandler(path,app,cfg))

	go func() {
		if err := (httpserver.Serve(tcpKeepAliveListener{listener.(*net.TCPListener)})); err != http.ErrServerClosed {
			log.Fatalln(err)
		}
	}()
	app.instance = httpserver
	return app, nil
}

func sendHandler(cookie http.Cookie, initCookie sync.Once, app *Server, cfg *config.Config)func(http.ResponseWriter, *http.Request){
	return func(w http.ResponseWriter, r *http.Request) {
		log.Println("header: ", r.Header)
		log.Println("agent: ",r.UserAgent())
		log.Println("cookies: ",r.Cookies())
		log.Println("cookie.Value = ", cookie.Value)
		if cookie.Value == "" {
			if !strings.HasPrefix(r.Header.Get("User-Agent"), "Mozilla") {
				log.Println("错误的User-Agent, 没有以Mozilla开头")
				http.Error(w, "", http.StatusBadRequest)
				app.stopChannel<-true
				return
			}
			initCookie.Do(func() {
				value, err := util.GetSessionID()
				if err != nil {
					log.Println("Unable to generate session ID", err)
					app.stopChannel <- true
					return
				}
				cookie.Value = value
				http.SetCookie(w, &cookie)
			})
		} else {
			// Check for the expected cookie and value
			// If it is missing or doesn't match
			// return a 404 status
			rcookie, err := r.Cookie(cookie.Name)
			if err != nil || rcookie.Value != cookie.Value {
				log.Printf("cookie不一致或者发生错误,err = `%+v`",err)
				http.Error(w, "", http.StatusNotFound)
				return
			}
		}
		w.Header().Set("Content-Disposition", "attachment; filename="+app.payload.Filename)
		w.Header().Set("Expires", "0")
		w.Header().Set("Cache-Control", "must-revalidate")
		w.Header().Set("Pragma", "public")
		http.ServeFile(w, r, app.payload.Path)
		log.Println("sendHandler结束")
		if !cfg.KeepAlive || !app.expectParallelRequests {
			app.stopChannel<-true
		}
	}
}
func receiveHandler(path string, app *Server, cfg *config.Config) func(http.ResponseWriter, *http.Request){
	return func (w http.ResponseWriter, r *http.Request) {
		htmlVariables := struct {
			Route string
			File  string
		}{}
		htmlVariables.Route = "/receive/" + path
		switch r.Method {
		case "POST":
			filenames := util.ReadFilenames(app.outputDir)
			reader, err := r.MultipartReader()
			if err != nil {
				fmt.Fprintf(w, "Upload error: %v\n", err)
				log.Printf("Upload error: %v\n", err)
				app.stopChannel <- true
				return
			}
			transferredFiles := []string{}
			progressBar := pb.New64(r.ContentLength)
			progressBar.ShowCounters = false
			for {
				part, err := reader.NextPart()
				if err == io.EOF {
					break
				}
				// iIf part.FileName() is empty, skip this iteration.
				if part.FileName() == "" {
					continue
				}
				// Prepare the destination
				fileName := getFileName(part.FileName(), filenames)
				out, err := os.Create(filepath.Join(app.outputDir, fileName))
				if err != nil {
					// Output to server
					fmt.Fprintf(w, "Unable to create the file for writing: %s\n", err)
					// Output to console
					log.Printf("Unable to create the file for writing: %s\n", err)
					// Send signal to server to shutdown
					app.stopChannel <- true
					return
				}
				defer out.Close()
				// Add name of new file
				filenames = append(filenames, fileName)
				// Write the content from POSTed file to the out
				fmt.Println("Transferring file: ", out.Name())
				progressBar.Prefix(out.Name())
				progressBar.Start()
				buf := make([]byte, 1024)
				for {
					// Read a chunk
					n, err := part.Read(buf)
					if err != nil && err != io.EOF {
						// Output to server
						fmt.Fprintf(w, "Unable to write file to disk: %v", err)
						// Output to console
						fmt.Printf("Unable to write file to disk: %v", err)
						// Send signal to server to shutdown
						app.stopChannel <- true
						return
					}
					if n == 0 {
						break
					}
					// Write a chunk
					if _, err := out.Write(buf[:n]); err != nil {
						// Output to server
						fmt.Fprintf(w, "Unable to write file to disk: %v", err)
						// Output to console
						log.Printf("Unable to write file to disk: %v", err)
						// Send signal to server to shutdown
						app.stopChannel <- true
						return
					}
					progressBar.Add(n)
				}
				transferredFiles = append(transferredFiles, out.Name())
			}
			progressBar.FinishPrint("File transfer completed")
			// Set the value of the variable to the actually transferred files
			htmlVariables.File = strings.Join(transferredFiles, ", ")
			serveTemplate("done", pages.Done, w, htmlVariables)
			if cfg.KeepAlive == false {
				app.stopChannel <- true
			}
		case "GET":
			serveTemplate("upload", pages.Upload, w, htmlVariables)
		}
	}
}