package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"path"

	oh "gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/client"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/server"
)

var (
	flagSet       = flag.NewFlagSet("oh", flag.ContinueOnError)
	multicastAddr = flagSet.String("a", "224.0.3.3", "multicast address")
	portNumber    = flagSet.Uint("t", 7432, "port number")
	interfaceName = flagSet.String("i", "eth0", "network interface to use")
	serverMode    = flagSet.Bool("s", false, "run in server mode")
	directory     string
	files         []string
)

func handleArgs() error {
	err := flagSet.Parse(os.Args[1:])
	if err != nil {
		return fmt.Errorf("error parsing arguments: %w", err)
	}

	if *multicastAddr == "" {
		return fmt.Errorf("multicast address is required")
	}

	ip := net.ParseIP(*multicastAddr)
	if ip == nil {
		return fmt.Errorf("invalid multicast address: %s", *multicastAddr)
	}

	if !ip.IsMulticast() {
		return fmt.Errorf("address is not a multicast address: %s", *multicastAddr)
	}

	if *portNumber > 65535 {
		return fmt.Errorf("invalid port number: %d", *portNumber)
	}

	if *interfaceName == "" {
		return fmt.Errorf("network interface is required")
	}

	if serverMode != nil && *serverMode {
		if flagSet.NArg() == 0 {
			return fmt.Errorf("at least one file to send is required")
		}

		if flagSet.NArg() > 1 { // we are expecting files on CLI
			files = flagSet.Args()
			err := checkFilesExist(files)
			if err != nil {
				return fmt.Errorf("error checking files: %w", err)
			}
		}

		// Just 1 arg
		fpath := flagSet.Args()[0]
		stat, err := os.Stat(fpath)
		if err != nil {
			return fmt.Errorf("error running stat on path %s: %w", fpath, err)
		}

		if !stat.IsDir() {
			files = flagSet.Args()
			err := checkFilesExist(files)
			if err != nil {
				return fmt.Errorf("error checking files: %w", err)
			}

			return nil
		}

		// path is a DIR
		entries, err := os.ReadDir(fpath)
		if err != nil {
			return fmt.Errorf("error reading dir: %w", err)
		}

		for _, e := range entries {
			if !e.IsDir() {
				files = append(files, path.Join(fpath, e.Name()))
			}
		}

		err = checkFilesExist(files)
		if err != nil {
			return fmt.Errorf("error checking files: %w", err)
		}

		return nil
	}

	// Client mode
	if flagSet.NArg() != 1 {
		return fmt.Errorf("directory to save files is required")
	}
	directory = flagSet.Arg(0)
	err = checkDirectoryExistsAndWritable(directory)
	if err != nil {
		return fmt.Errorf("error checking directory: %w", err)
	}

	return nil
}

func checkFilesExist(files []string) error {
	for _, file := range files {
		_, err := os.Stat(file)
		if os.IsNotExist(err) {
			return fmt.Errorf("file does not exist: %s", file)
		} else if err != nil {
			return fmt.Errorf("unable to access file: %s", file)
		}
	}

	return nil
}

func checkDirectoryExistsAndWritable(dir string) error {
	info, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return fmt.Errorf("directory does not exist: %s", dir)
	} else if err != nil {
		return fmt.Errorf("unable to access directory: %s", dir)
	}

	if !info.IsDir() {
		return fmt.Errorf("path is not a directory: %s", dir)
	}

	testFile := path.Join(dir, ".mftpc_test")
	f, err := os.Create(testFile)
	if err != nil {
		return fmt.Errorf("directory is not writable: %s", dir)
	}

	err = f.Close()
	if err != nil {
		return fmt.Errorf("closing test file: %w", err)
	}

	err = os.Remove(testFile)
	if err != nil {
		return fmt.Errorf("removing test file: %w", err)
	}
	return nil
}

func main() {
	err := handleArgs()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
		flagSet.Usage()
		os.Exit(1)
	}

	if *serverMode {
		err = runServer()
	} else {
		err = runClient()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
		os.Exit(2)
	}

}

func runServer() error {
	conf, err := oh.NewConfig(net.ParseIP(*multicastAddr), uint16(*portNumber), *interfaceName)
	if err != nil {
		return fmt.Errorf("error creating config: %w", err)
	}
	m, err := server.New(conf)
	if err != nil {
		return fmt.Errorf("error creating MFTP server: %w", err)
	}

	err = m.SendFiles(files)
	if err != nil {
		return fmt.Errorf("error sending files: %w", err)
	}

	return nil
}

func runClient() error {
	conf, err := oh.NewConfig(net.ParseIP(*multicastAddr), uint16(*portNumber), *interfaceName)
	if err != nil {
		return fmt.Errorf("error creating config: %w", err)
	}
	c, err := client.New(conf)
	if err != nil {
		return fmt.Errorf("error creating MFTP client: %w", err)
	}

	err = c.ReceiveFiles(directory)
	if err != nil {
		return fmt.Errorf("error receiving files: %w", err)
	}

	return nil
}
